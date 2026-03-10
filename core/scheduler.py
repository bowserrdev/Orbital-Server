"""
scheduler.py — Orbital Background Scheduler (5° processo)
Ottimizzato per: PostgreSQL 18 · io_uring · ARM Ampere · 24GB RAM

Processo autonomo: nessun import di FastAPI.
Gestisce DRR, distribuisce job ai worker interni, notifica via pg_notify.

Fix applicati rispetto alla versione precedente:
  [1] task_done() ora in finally annidato — garantisce chiamata anche se
      _notify_and_archive solleva CancelledError o altri errori.
  [2] slots_free calcolato tramite active_ctx['count'] invece di qsize().
      qsize() conta solo i job in attesa, non quelli già presi dai worker.
      Con qsize() e tutti i worker occupati si potevano avere 2×CONCURRENCY
      job simultanei. Ora il contatore traccia i job realmente in volo.
  [3] _shutdown_event spostato dentro main() — asyncio.Event() creato
      fuori da un loop attivo è deprecato in Python 3.10+ e rompe con uvloop.
  [4] await asyncio.sleep(0) alla fine di ogni ciclo del loop — cede
      il controllo all'event loop anche quando ci sono job da processare,
      evitando busy-wait che affama gli altri task asyncio.
"""

import asyncio
import json
import signal
import time
from loguru import logger

from database import queue_db

# ------------------------------------------------------------------
# Configurazione
# ------------------------------------------------------------------

# Scraping paralleli massimi.
# Limite reale: rate limit DMM/TMDB, non CPU né RAM.
# Aumentare proporzionalmente ai proxy Cloudflare Workers disponibili.
CONCURRENCY_LIMIT = 20

# Intervallo fisso tra quantum DRR consecutivi (secondi).
QUANTUM_INTERVAL_SECONDS    = 1.0

# Soglia per considerare un job orfano.
STALE_JOB_THRESHOLD_MINUTES = 10

# Intervalli task di manutenzione.
RECOVERY_INTERVAL_SECONDS   = 60.0
CLEANUP_INTERVAL_SECONDS    = 3600.0


# ------------------------------------------------------------------
# Worker interni
# ------------------------------------------------------------------

async def scraping_worker(
    worker_id: int,
    job_queue: asyncio.Queue,
    active_ctx: dict,
):
    """
    Loop infinito per ogni worker:
      1. Attende un job dalla asyncio.Queue in RAM.
      2. Incrementa active_ctx['count'] per segnalare al loop che
         questo worker è occupato (fix: slots_free era calcolato su
         qsize() che non conta i job già estratti dalla queue).
      3. Esegue lo scraping (logica di business).
      4. Notifica subscriber via pg_notify + archivia in jobs_history.
      5. Decrementa active_ctx['count'] e chiama task_done() in un
         finally annidato — garantisce l'esecuzione anche se
         _notify_and_archive solleva eccezione o CancelledError.

    La asyncio.Queue è il buffer in RAM tra Scheduler e Worker.
    Zero overhead DB per il passaggio interno dei job.

    job_subscriptions e jobs_active vengono eliminati atomicamente
    dalla Super Query al momento dell'acquisizione.
    Il worker riceve solo: job_id, imdb_id, media_type, channels[].
    """
    logger.info(f"👷 Worker {worker_id} pronto")

    while True:
        job = await job_queue.get()

        # FIX [2]: incrementa il contatore prima di iniziare il lavoro.
        # Il decremento avviene nel finally annidato più in basso.
        active_ctx["count"] += 1

        job_id   = job["job_id"]
        imdb_id  = job["imdb_id"]
        channels = job["channels"]

        logger.debug(f"[W{worker_id}] ▶ {imdb_id}")
        status = "failed"

        try:
            # -------------------------------------------------------
            # LOGICA DI BUSINESS — da implementare nei moduli dedicati
            # -------------------------------------------------------
            #   1. metadata = await metadata_client.fetch(imdb_id)
            #      → TMDB / TVDB via httpx + APIFactory singleton
            #
            #   2. results = await dmm_client.search(imdb_id)
            #      → Debrid Media Manager via Cloudflare Workers
            #        (parallelizzazione per aggirare i 429)
            #
            #   3. parsed = await loop.run_in_executor(
            #          rust_executor,           # ProcessPoolExecutor dedicato
            #          rust_ptn.parse_batch,    # modulo PyO3/Maturin
            #          results.raw_filenames    # list[str], anche migliaia
            #      )
            #      → PTN parsing in Rust per serie TV con molte stagioni
            #        (operazione CPU-bound, fuori dall'event loop)
            #
            #   4. await torrents_db.bulk_insert(parsed)
            #      → asyncpg COPY per INSERT massivo su tabella torrents
            # -------------------------------------------------------
            await asyncio.sleep(2.0)   # placeholder
            status = "success"
            logger.debug(f"[W{worker_id}] ✓ {imdb_id}")

        except Exception as e:
            logger.error(f"[W{worker_id}] ✗ {imdb_id}: {e}")

        finally:
            # FIX [1]: task_done() in un finally annidato.
            # Se _notify_and_archive solleva (es. CancelledError, pool None),
            # task_done() viene comunque chiamata e job_queue.join()
            # non si blocca per sempre durante lo shutdown.
            active_ctx["count"] -= 1
            try:
                await _notify_and_archive(
                    worker_id, job_id, imdb_id, channels, status
                )
            finally:
                job_queue.task_done()


async def _notify_and_archive(
    worker_id: int,
    job_id,
    imdb_id: str,
    channels: list[str],
    status: str,
):
    """
    Singola transazione:
      1. pg_notify a tutti i canali subscriber.
         SELECT pg_notify($1, $2) con parametri asyncpg: nessuna
         concatenazione di stringhe, nessuna SQL injection possibile.
      2. INSERT in jobs_history.

    Payload volutamente minimo (imdb_id + status):
    il worker FastAPI che riceve il NOTIFY farà la SELECT su torrents
    per i dati completi. Mantiene il payload sotto il limite 8000 byte
    di pg_notify anche per titoli con nomi lunghi.
    """
    payload = json.dumps({"imdb_id": imdb_id, "status": status})

    try:
        async with queue_db.pool.acquire() as conn:
            async with conn.transaction():

                for channel in channels:
                    await conn.execute(
                        "SELECT pg_notify($1, $2)",
                        channel,
                        payload,
                    )

                await conn.execute(
                    """
                    INSERT INTO jobs_history (id, imdb_id, result_status)
                    VALUES ($1, $2, $3)
                    """,
                    job_id,
                    imdb_id,
                    status,
                )

    except Exception as e:
        logger.critical(
            f"[W{worker_id}] ERRORE CRITICO cleanup {job_id} "
            f"({imdb_id}): {e}"
        )


# ------------------------------------------------------------------
# Scheduler loop
# ------------------------------------------------------------------

async def scheduler_loop(job_queue: asyncio.Queue, active_ctx: dict):
    """
    Loop principale dello Scheduler.

    Calcolo slots_free — fix [2]:
      La versione precedente usava:
          slots_free = CONCURRENCY_LIMIT - job_queue.qsize()
      qsize() conta i job in attesa nella queue, NON quelli già estratti
      da un worker e in elaborazione. Con tutti i 20 worker occupati e
      queue vuota si otteneva slots_free=20, portando a 40 job simultanei.

      Fix: slots_free = CONCURRENCY_LIMIT - active_ctx['count']
      active_ctx['count'] viene incrementato da ogni worker appena
      acquisisce un job (prima dell'elaborazione) e decrementato nel
      finally. Rappresenta i job realmente in volo in ogni momento.

    Estrazione batch DRR in un singolo round-trip DB:
      slots_free job vengono estratti in una sola transazione con N
      iterazioni della CTE (tutte dentro la stessa transazione del caller).

    Timer separati con time.monotonic():
      · Quantum DRR  ogni QUANTUM_INTERVAL_SECONDS  (1s)
      · Recovery     ogni RECOVERY_INTERVAL_SECONDS (60s)
      · Cleanup      ogni CLEANUP_INTERVAL_SECONDS  (3600s)

    Fix [4] — yield all'event loop:
      await asyncio.sleep(0) alla fine di ogni ciclo cede il controllo
      all'event loop anche quando ci sono job da processare, evitando
      che il loop monopolizzi il thread asyncio.
    """
    logger.info("🧠 Scheduler loop avviato")

    last_quantum  = 0.0
    last_recovery = 0.0
    last_cleanup  = 0.0

    while True:
        now = time.monotonic()

        # -- Quantum periodico -------------------------------------------
        if now - last_quantum >= QUANTUM_INTERVAL_SECONDS:
            await queue_db.add_quantum_to_active_users()
            last_quantum = now

        # -- Recovery job orfani -----------------------------------------
        if now - last_recovery >= RECOVERY_INTERVAL_SECONDS:
            await queue_db.recover_stale_jobs(STALE_JOB_THRESHOLD_MINUTES)
            last_recovery = now

        # -- Cleanup utenti inattivi -------------------------------------
        if now - last_cleanup >= CLEANUP_INTERVAL_SECONDS:
            await queue_db.cleanup_inactive_users()
            last_cleanup = now

        # -- Calcola slot liberi -----------------------------------------
        # FIX [2]: usa active_ctx['count'] invece di job_queue.qsize().
        # active_ctx['count'] = job attualmente in elaborazione dai worker.
        # slots_free = slot disponibili senza sforare CONCURRENCY_LIMIT.
        slots_free = CONCURRENCY_LIMIT - active_ctx["count"]

        if slots_free <= 0:
            # Tutti i worker sono occupati — backpressure
            await asyncio.sleep(0.1)
            continue

        # -- Estrazione batch DRR in un singolo round-trip ---------------
        async with queue_db.pool.acquire() as conn:
            async with conn.transaction():
                jobs = await queue_db.fetch_and_claim_next_jobs(
                    conn,
                    batch_size=slots_free,
                )

        if jobs:
            for job in jobs:
                await job_queue.put(job)
            logger.debug(
                f"🎯 Batch estratto: {len(jobs)} job "
                f"({', '.join(j['imdb_id'] for j in jobs)})"
            )
        else:
            # Coda DB vuota o tutti i job restanti locked
            await asyncio.sleep(0.5)
            continue

        # FIX [4]: cede il controllo all'event loop dopo ogni ciclo attivo.
        # Senza questo sleep(0) il loop gira a velocità massima quando ci
        # sono job, affamando gli altri task asyncio (worker, I/O).
        await asyncio.sleep(0)


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

async def main():
    """
    Avvio ordinato:
      1. Signal handlers (SIGTERM per Docker stop, SIGINT per Ctrl+C)
         FIX [3]: _shutdown_event creato dentro main() invece che a livello
         di modulo. asyncio.Event() fuori da un loop attivo è deprecato
         in Python 3.10+ e causa problemi con uvloop.
      2. Connessione DB + schema
      3. Contatore condiviso active_ctx per tracking job in volo (fix [2])
      4. Pool worker asincroni
      5. Scheduler loop
      6. Shutdown graceful: job_queue.join() aspetta che i worker
         finiscano i job già acquisiti prima di terminare.
         Nessuno scraping viene interrotto a metà.
    """
    # FIX [3]: Event creato dentro il loop attivo — compatibile con uvloop.
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def _on_shutdown():
        logger.info("🛑 Segnale shutdown ricevuto — attendo completamento job...")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_shutdown)

    await queue_db.connect()

    # Contatore condiviso job in volo — dizionario per mutabilità via closure.
    # asyncio è single-thread: nessuna race condition, nessun lock necessario.
    # FIX [2]: sostituisce job_queue.qsize() per il calcolo di slots_free.
    active_ctx: dict = {"count": 0}

    # maxsize=CONCURRENCY_LIMIT implementa la backpressure sulla queue:
    # serve solo come buffer di transito tra scheduler e worker.
    # Il vero limite di concorrenza è active_ctx['count'].
    job_queue = asyncio.Queue(maxsize=CONCURRENCY_LIMIT)

    worker_tasks = [
        asyncio.create_task(
            scraping_worker(i, job_queue, active_ctx),
            name=f"scraping-worker-{i}",
        )
        for i in range(CONCURRENCY_LIMIT)
    ]
    logger.info(f"✅ {CONCURRENCY_LIMIT} worker avviati")

    scheduler_task = asyncio.create_task(
        scheduler_loop(job_queue, active_ctx),
        name="scheduler-loop",
    )

    # DOPO
    await shutdown_event.wait()

    # Shutdown ordinato
    # cancel() posta solo la richiesta — await garantisce che la task
    # abbia effettivamente sollevato CancelledError e terminato
    # prima di procedere con job_queue.join().
    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        pass

    logger.info("⏳ Attendo completamento job in lavorazione...")
    await job_queue.join()

    for task in worker_tasks:
        task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)

    await queue_db.close()
    logger.info("✅ Shutdown completato")


if __name__ == "__main__":
    asyncio.run(main())
