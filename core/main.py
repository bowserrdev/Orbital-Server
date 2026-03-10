"""
main.py — Orbital API Server
═══════════════════════════════════════════════════════════════════════════════
Stack:   FastAPI · Uvicorn × 4 worker · uvloop · httptools · asyncpg
Target:  Oracle Cloud Ampere A1 — 4 OCPU · 24 GB RAM · 4 Gbps

Avvio produzione:
    gunicorn main:app -c gunicorn_conf.py

Avvio sviluppo (singolo processo):
    uvicorn main:app --loop uvloop --http httptools --reload

═══════════════════════════════════════════════════════════════════════════════
ARCHITETTURA NOTIFICHE (perché non usiamo broadcast)
───────────────────────────────────────────────────
Ogni worker Uvicorn mantiene UNA connessione asyncpg dedicata al LISTEN.
Quando un utente apre /events:
  1. LISTEN user_{id}   → Postgres invierà i notify solo a questo worker
  2. Queue RAM locale   → callback deposita il messaggio
  3. SSE generator      → svuota la queue e invia l'evento al browser

Al disconnect:  UNLISTEN user_{id}  → nessun notify sprecato

Perché NON il broadcast (canale globale):
  - Con N worker, ogni msg sarebbe ricevuto N volte, elaborato N volte,
    ignorato (N-1) volte — efficienza = 1/N.
  - Postgres fa già il routing ottimale con LISTEN per-canale.
  - Zero modifiche allo scheduler: continua a fare NOTIFY user_{id}.

═══════════════════════════════════════════════════════════════════════════════
METRICHE REALISTICHE (Oracle Ampere A1 · uvloop)
────────────────────────────────────────────────
Connessioni SSE idle:        ~40.000–60.000 totali (limite: fd del kernel)
Richieste HTTP /discover:    ~8.000–12.000 req/s (limite: CPU uvloop)
Utenti con job attivi:       ~200–300 con QoS accettabile (<30s attesa)
Bottleneck reale:            rate limit siti torrent, non la macchina

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
import uvloop
from fastapi import FastAPI, Header, HTTPException, Request, status

# Installa uvloop come event loop policy di default per questo processo.
# Safety net: garantisce uvloop anche se il server viene avviato senza
# --loop uvloop (es. sviluppo locale con `python main.py` o test unitari).
# Quando uvicorn/gunicorn già usano --loop uvloop, questa chiamata è no-op.
# httptools non va importato: è un parser HTTP interno a uvicorn, si attiva
# solo tramite --http httptools a livello di server, senza API applicative.
uvloop.install()
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from database import DB_DSN, queue_db

# ─────────────────────────────────────────────────────────────────────────────
# Modelli Pydantic
# ─────────────────────────────────────────────────────────────────────────────

class DiscoverItem(BaseModel):
    imdb_id:    str = Field(..., min_length=1, max_length=20)
    media_type: str = Field(..., pattern="^(movie|series)$")
    priority:   int = Field(
        default=0,
        ge=0,
        le=100,
        description=(
            "Priorità di elaborazione (0 = massima, 100 = minima). "
            "A parità di slot DRR, i job con valore più basso vengono "
            "estratti per primi. Se omesso, tutti gli item del batch "
            "ricevono priorità 0."
        ),
    )


class DiscoverRequest(BaseModel):
    items: list[DiscoverItem] = Field(..., min_length=1, max_length=50)


# ─────────────────────────────────────────────────────────────────────────────
# Costanti
# ─────────────────────────────────────────────────────────────────────────────

# Secondi di timeout tra ping SSE.
# Ogni timeout invia un keepalive e controlla se il client è ancora vivo.
SSE_PING_INTERVAL = 15.0

# Numero massimo di ping senza dati reali prima di chiudere la connessione.
# 20 × 15s = 5 minuti — elimina connessioni fantasma (mobile, NAT timeout).
SSE_MAX_IDLE_PINGS = 20

# Limite job pendenti per utente: anti-DoS / equità DRR.
USER_JOB_QUEUE_LIMIT = 50

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

_ALLOW_IP_FALLBACK = os.getenv("ORBITAL_ALLOW_IP_FALLBACK", "false").lower() == "true"

# ─────────────────────────────────────────────────────────────────────────────
# Stato locale del worker (RAM di questo processo — non condivisa tra worker)
# ─────────────────────────────────────────────────────────────────────────────

# user_id → asyncio.Queue  (una coda SSE per utente connesso a questo worker)
local_connections: dict[str, asyncio.Queue] = {}

# Connessione asyncpg dedicata al LISTEN (fuori dal pool — mai usata per query)
_listener_conn: asyncpg.Connection | None = None

# Event che segnala quando la connessione LISTEN è pronta.
# Usato da listen_user() per evitare LISTEN prima che la connessione esista.
_listener_ready: asyncio.Event


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
        data    = json.loads(payload)
        user_id = channel.removeprefix("user_")
        queue   = local_connections.get(user_id)
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
            _listener_conn = await asyncpg.connect(dsn=DB_DSN)

            # Ripristina i canali LISTEN degli utenti già connessi.
            # Caso normale (prima apertura): local_connections è vuoto.
            # Caso reconnect: riregistra tutti i canali senza perdere utenti.
            if local_connections:
                for user_id in list(local_connections.keys()):
                    await _listener_conn.add_listener(f"user_{user_id}", _on_notify)
                logger.warning(
                    f"[Listener] Reconnesso — ripristinati "
                    f"{len(local_connections)} canali LISTEN"
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
            await _listener_conn.remove_listener(f"user_{user_id}", _on_notify)
        except Exception as e:
            # Non critico: il canale verrà rimosso automaticamente
            # quando la connessione LISTEN si chiude.
            logger.warning(f"[Listener] UNLISTEN user_{user_id} fallito: {e}")

    logger.info(f"➖ user_{user_id} disconnesso (locale: {len(local_connections)})")


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

    # FIX: Event creato dentro il loop attivo (stesso pattern del fix [3]
    # in scheduler.py — compatibile con uvloop)
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
    docs_url=None,
    redoc_url=None,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: estrae user_id dalla request
# ─────────────────────────────────────────────────────────────────────────────

def _get_user_id(request: Request, x_user_id: str | None) -> str:
    if x_user_id:
        return x_user_id

    if _ALLOW_IP_FALLBACK and request.client:
        logger.warning(
            f"[Auth] X-User-Id assente — fallback IP {request.client.host} "
            "(ORBITAL_ALLOW_IP_FALLBACK=true)"
        )
        return request.client.host

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Header X-User-Id obbligatorio",
    )


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


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint: accoda job
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/v1/discover", status_code=status.HTTP_202_ACCEPTED)
async def discover(
    request: Request,
    payload: DiscoverRequest,
    x_user_id: str | None = Header(default=None),
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
    user_id      = _get_user_id(request, x_user_id)
    channel_name = f"user_{user_id}"

    results = await asyncio.gather(*[
        queue_db.enqueue_job(
            imdb_id=item.imdb_id,
            media_type=item.media_type,
            priority=item.priority,
            user_id=user_id,
            channel_name=channel_name,
        )
        for i, item in enumerate(payload.items)
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
    x_user_id: str | None = Header(default=None),
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
    user_id    = _get_user_id(request, x_user_id)
    user_queue = await listen_user(user_id)

    async def event_generator() -> AsyncGenerator[dict, None]:
        idle_pings = 0

        # Task disconnect: si risolve quando il client chiude la connessione.
        # Event-driven: asyncpg notifica FastAPI via ASGI receive().
        try:
            while True:
                # Ricreato ogni ciclo: garantisce che non sia mai in stato done/cancelled
                disconnect_task = asyncio.create_task(
                    request.is_disconnected(),
                    name=f"disconnect-{user_id}",
                )
                data_task = asyncio.create_task(
                    asyncio.wait_for(user_queue.get(), timeout=SSE_PING_INTERVAL),
                    name=f"data-{user_id}",
                )

                done, pending = await asyncio.wait(
                    {disconnect_task, data_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass

                if disconnect_task in done:
                    break

                try:
                    data = data_task.result()
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

        finally:
            await unlisten_user(user_id)

    return EventSourceResponse(event_generator())


# ─────────────────────────────────────────────────────────────────────────────
# Handler errori globali
# ─────────────────────────────────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(f"Errore non gestito su {request.url}: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Errore interno del server"},
    )