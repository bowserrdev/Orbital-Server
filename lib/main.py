from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, TYPE_CHECKING
import json
from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse, RedirectResponse
from sse_starlette.sse import EventSourceResponse, BackgroundTask
if TYPE_CHECKING: import asyncpg

from database import queue_db
from logger import logger
from models import DiscoverRequest

# ─────────────────────────────────────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────────────────────────────────────

# Secondi di timeout tra ping SSE.
# Ogni timeout invia un keepalive e controlla se il client è ancora vivo.
SSE_PING_INTERVAL = 15.0

# Numero massimo di ping senza dati reali prima di chiudere la connessione.
# 20 × 15s = 5 minuti — elimina connessioni fantasma (mobile, NAT timeout).
SSE_MAX_IDLE_PINGS = 20


# Capacità massima della queue SSE in RAM per utente.
# put_nowait() solleva QueueFull se il client è troppo lento a consumare.
SSE_QUEUE_MAXSIZE = 100

# Secondi di attesa iniziale per la connessione LISTEN prima di
# accettare traffico SSE (evita race condition all'avvio).
LISTENER_READY_WAIT = 0.3

# Secondi tra heartbeat sul listener (verifica che la connessione sia viva).
LISTENER_HEARTBEAT_INTERVAL = 30.0

# Secondi tra tentativi di riconnessione del listener dopo un crash.
LISTENER_RETRY_DELAY = 2.0

# ─────────────────────────────────────────────────────────────────────────────
# Stato locale del worker (RAM di questo processo — non condivisa tra worker)
# ─────────────────────────────────────────────────────────────────────────────

# user_id → asyncio.Queue  (una coda SSE per utente connesso a questo worker)
local_connections: dict[str, asyncio.Queue] = {}

# Event che segnala quando la connessione LISTEN è pronta.
# Usato da listen_user() per evitare LISTEN prima che la connessione esista.
_listener_ready: asyncio.Event

# ─────────────────────────────────────────────────────────────────────────────
# Lifespan
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Avvio e shutdown ordinato di ogni worker Uvicorn.

    Avvio:
      1. _listener_ready Event (deve vivere dentro un loop attivo — come
         asyncio.Event in scheduler.py, evita il problema uvloop)
      2. Pool asyncpg per le query normali
      3. Task listener LISTEN in background

    Shutdown:
      1. Cancella il listener (chiude la connessione LISTEN)
      2. Chiude il pool asyncpg
    """
    global _listener_ready

    _listener_ready = asyncio.Event()

    await queue_db.connect()

    listener_task = asyncio.create_task(start_listener(), name="pg-listener")

    # Attende che la connessione LISTEN sia pronta prima di accettare traffico.
    # Timeout breve: se Postgres non risponde in 5s c'è un problema più grave.
    try:
        await asyncio.wait_for(_listener_ready.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        logger.error(
            "[Lifespan] Listener non pronto in 5s — "
            "il server accetta richieste ma /events restituirà 503 "
            "finché Postgres non risponde"
        )

    yield

    # Shutdown ordinato
    listener_task.cancel()
    try:
        await listener_task
    except asyncio.CancelledError:
        pass

    await queue_db.close()
    logger.info("[Lifespan] Worker shutdown completato")

# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Orbital API",
    lifespan=lifespan,
    # Disabilita la documentazione automatica in produzione
    # per ridurre overhead e superficie di attacco.
    # Riabilitare in sviluppo: docs_url="/docs"
    docs_url="/docs",
    redoc_url="/redoc"
)

# ─────────────────────────────────────────────────────────────────────────────
# Callback LISTEN (modulo-level: stessa istanza usata da add/remove_listener)
# ─────────────────────────────────────────────────────────────────────────────

def _on_notify(conn: asyncpg.Connection, pid: int, channel: str, payload: str) -> None:
    """
    Callback sincrona invocata da asyncpg nel thread asyncio ad ogni NOTIFY.

    channel  == "user_{user_id}" — il routing è già fatto da Postgres:
                 questa callback viene chiamata SOLO se l'utente è locale.
    payload  == JSON: {"imdb_id": "...", "status": "success|failed"}

    Non fa I/O: deposita il messaggio nella queue RAM dell'utente.
    Se la queue è piena (client lento) scarta silenziosamente — il client
    vedrà solo i risultati successivi; nessun crash del listener.
    """
    try:
        data = json.loads(payload)
        user_id = channel.removeprefix("user_")
        queue = local_connections.get(user_id)
        if queue is not None:
            queue.put_nowait(data)
    except asyncio.QueueFull:
        logger.warning(f"[Listener] Queue piena per user_{user_id} — notify scartato")
    except Exception as e:
        logger.error(f"[Listener] Errore callback: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Listener asincrono — una connessione per worker
# ─────────────────────────────────────────────────────────────────────────────

async def start_listener() -> None:
    """
    Task di background: mantiene viva la connessione LISTEN per questo worker.

    Gestisce automaticamente:
      · Apertura iniziale della connessione
      · Heartbeat periodico (SELECT 1 ogni LISTENER_HEARTBEAT_INTERVAL secondi)
      · Riconnessione automatica se Postgres cade o la connessione è persa
      · Ripristino di tutti i canali LISTEN degli utenti già connessi
        dopo una riconnessione (evita notify silenziosamente persi)

    La connessione LISTEN è separata dal pool asyncpg: non compete
    con le query normali e non viene mai restituita al pool.
    """
    global _listener_conn

    while True:
        try:
            _listener_conn = await queue_db.connect_listener()

            # Ripristina i canali LISTEN degli utenti già connessi.
            # Caso normale (prima apertura): local_connections è vuoto.
            # Caso reconnect: riregistra tutti i canali senza perdere utenti.
            if local_connections:
                for user_id in list(local_connections.keys()):
                    await _listener_conn.add_listener(f"user_{user_id}", _on_notify)
                logger.warning(
                    f"[Listener] Riconnesso — ripristinati {len(local_connections)} canali LISTEN"
                )
            else:
                logger.info("[Listener] Connessione LISTEN pronta")

            _listener_ready.set()

            # Heartbeat loop: verifica che la connessione sia viva.
            # asyncpg non rilevaa automaticamente le connessioni zombie
            # (TCP keepalive dipende dalla config del kernel/proxy).
            while not _listener_conn.is_closed():
                await asyncio.sleep(LISTENER_HEARTBEAT_INTERVAL)
                try:
                    await _listener_conn.execute("SELECT 1")
                except Exception:
                    logger.warning("[Listener] Heartbeat fallito — riconnessione...")
                    break

        except asyncio.CancelledError:
            # Shutdown ordinato: esci senza retry
            break

        except Exception as e:
            logger.error(
                f"[Listener] Connessione persa: {e} — "
                f"retry in {LISTENER_RETRY_DELAY}s"
            )
            _listener_ready.clear()
            await asyncio.sleep(LISTENER_RETRY_DELAY)

        finally:
            if _listener_conn and not _listener_conn.is_closed():
                await _listener_conn.close()
            _listener_conn = None
            _listener_ready.clear()

# ─────────────────────────────────────────────────────────────────────────────
# LISTEN / UNLISTEN per-utente
# ─────────────────────────────────────────────────────────────────────────────

async def listen_user(user_id: str) -> asyncio.Queue:
    """
    Registra un utente: crea la sua queue SSE e apre il canale LISTEN.
    Attende che la connessione listener sia pronta (con timeout).

    Chiamata da GET /events all'apertura della connessione SSE.
    """
    # Timeout di attesa listener: evita che l'utente aspetti all'infinito
    # se il listener non riesce a connettersi a Postgres.
    try:
        await asyncio.wait_for(_listener_ready.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servizio temporaneamente non disponibile — riprova tra qualche secondo",
        )

    queue: asyncio.Queue = asyncio.Queue(maxsize=SSE_QUEUE_MAXSIZE)
    local_connections[user_id] = queue

    if _listener_conn and not _listener_conn.is_closed():
        await _listener_conn.add_listener(f"user_{user_id}", _on_notify)

    logger.info(f"➕ user_{user_id} connesso (locale: {len(local_connections)})")
    return queue


async def unlisten_user(user_id: str) -> None:
    """
    Rimuove la registrazione utente: cancella la queue e fa UNLISTEN.
    UNLISTEN libera il canale su Postgres — nessun notify sprecato.

    Chiamata dal finally del generator SSE alla disconnessione.
    Sicura da chiamare anche se l'utente non era registrato.
    """
    local_connections.pop(user_id, None)

    if _listener_conn and not _listener_conn.is_closed():
        try:
            # shield protegge questa coroutine dalla cancellazione del task padre.
            # Senza shield, CancelledError viene rilanciato sull'await mentre
            # il task è ancora in stato di cancellazione, interrompendo l'UNLISTEN.
            await asyncio.shield(
                _listener_conn.remove_listener(f"user_{user_id}", _on_notify)
            )
        except asyncio.CancelledError:
            # shield assorbe la cancellazione esterna — l'UNLISTEN è già partito
            # e verrà completato. Non rilanciare.
            pass
        except Exception as e:
            logger.warning(f"[Listener] UNLISTEN user_{user_id} fallito: {e}")

    logger.info(f"➖ user_{user_id} disconnesso (locale: {len(local_connections)})")


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint: accoda job
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/v1/discover", status_code=status.HTTP_202_ACCEPTED)
async def discover(
    request: Request,
    payload: DiscoverRequest,
    user_id: str = Header(..., alias="x-user-id", pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
) -> JSONResponse:
    """
    Accoda fino a 50 job per l'utente in una singola richiesta.

    Ogni item viene processato in parallelo (asyncio.gather) — N job
    = 1 round-trip DB per job (la transazione di enqueue_job è atomica).

    enqueue_job (database.py Fix 2) gestisce:
      · UPSERT scheduler_state (garantisce che DRR conosca l'utente)
      · INSERT jobs_active ON CONFLICT DO NOTHING (idempotente)
      · INSERT job_subscriptions (collega utente → job → canale pg_notify)
      · Rate limiting: rifiuta se l'utente ha già USER_JOB_QUEUE_LIMIT job pendenti

    Risposta 202 Accepted: il job è in coda, non ancora completato.
    L'utente riceverà il risultato via SSE quando lo scheduler lo elabora.
    """

    results = await asyncio.gather(*[
        queue_db.enqueue_job(
            user_id=user_id,
            item=item.model_dump(),
            debrid_services=json.dumps(payload.debrid_services)
        )
        for item in payload.items
    ])

    queued      = sum(1 for r in results if r.get("created"))
    already_in  = sum(1 for r in results if not r.get("created") and r.get("job_id"))
    rate_limited = sum(1 for r in results if not r.get("job_id"))

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "status":       "accepted",
            "queued":       queued,
            "already_queued": already_in,
            "rate_limited": rate_limited,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint: SSE stream
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/v1/events")
async def events(
    request: Request,
    user_id: str = Header(..., alias="x-user-id", pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
) -> EventSourceResponse:
    """
    Server-Sent Events — push notifiche di completamento job.

    Flusso completo:
      1. GET /events                    → apertura connessione SSE
      2. listen_user()                  → LISTEN user_{id} su Postgres + queue RAM
      3. POST /discover                 → job accodato
      4. Scheduler elabora             → NOTIFY user_{id} con payload JSON
      5. _on_notify() callback          → queue.put_nowait(data)
      6. event_generator()              → yield {"event": "result", "data": ...}
      7. Browser chiude / timeout idle  → finally → unlisten_user() → UNLISTEN

    Disconnect detection event-driven (non polling):
      asyncio.wait(FIRST_COMPLETED) tra il task dati e il task disconnect
      evita di chiamare request.is_disconnected() ad ogni tick — con
      migliaia di connessioni idle il polling sarebbe CPU sprecata.

    Idle timeout:
      SSE_MAX_IDLE_PINGS × SSE_PING_INTERVAL (default: 5 minuti).
      Chiude connessioni fantasma (mobile che perde rete, NAT timeout)
      senza aspettare il TCP keepalive del kernel.
    """
    user_queue = await listen_user(user_id)

    async def event_generator() -> AsyncGenerator[dict, None]:
        idle_pings = 0
        try:
            while True:
                try:
                    data = await asyncio.wait_for(
                        user_queue.get(), 
                        timeout=SSE_PING_INTERVAL
                    )
                    idle_pings = 0
                    yield {"event": "result", "data": json.dumps(data)}

                except asyncio.TimeoutError:
                    idle_pings += 1
                    if idle_pings >= SSE_MAX_IDLE_PINGS:
                        logger.info(
                            f"[SSE] user_{user_id} idle da "
                            f"{idle_pings * SSE_PING_INTERVAL:.0f}s — chiuso"
                        )
                        break
                    yield {"event": "ping", "data": ""}

        except asyncio.CancelledError:
            # sse-starlette cancella questo generator quando il client
            # chiude la connessione. È il meccanismo corretto, non un errore.
            logger.info(f"[SSE] user_{user_id} disconnesso")

        finally:
            await unlisten_user(user_id)  # eseguito sempre, anche su CancelledError

    return EventSourceResponse(event_generator())

# ─────────────────────────────────────────────────────────────────────────────
# Utils
# ─────────────────────────────────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(f"Errore non gestito su {request.url}: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Errore interno del server"},
    )

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")

# ─────────────────────────────────────────────────────────────────────────────
# Endpoint: health check
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health", include_in_schema=False)
async def health() -> JSONResponse:
    """
    Usato da Docker HEALTHCHECK e load balancer.
    Verifica che il pool DB sia vivo e il listener sia connesso.
    """
    db_ok       = queue_db.pool is not None and not queue_db.pool._closed
    listener_ok = _listener_conn is not None and not _listener_conn.is_closed()

    if not db_ok or not listener_ok:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status":   "degraded",
                "db":       db_ok,
                "listener": listener_ok,
            },
        )

    return JSONResponse(content={
        "status":      "ok",
        "db":          True,
        "listener":    True,
        "connections": len(local_connections),
    })

if __name__ == "__main__":
    import uvicorn
    # Lancia il server solo se esegui questo file direttamente
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)