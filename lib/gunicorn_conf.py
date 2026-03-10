"""
gunicorn_conf.py — Configurazione produzione Orbital
═══════════════════════════════════════════════════════
Oracle Cloud Ampere A1 — 4 OCPU · 24 GB RAM

Avvio:
    gunicorn main:app -c gunicorn_conf.py

Note:
  · workers=4 → 1 per OCPU Ampere (workload I/O bound, non CPU bound)
  · uvloop + httptools dichiarati qui; UvicornWorker li passa a uvicorn
  · worker_connections=10000 → fd per worker (richiede ulimits nel docker-compose)
  · timeout=0 → disabilitato: le connessioni SSE sono long-lived per design
  · keepalive=5 → HTTP keepalive per riuso connessioni su /discover
"""


# ─── Worker ───────────────────────────────────────────────────────────────────

workers      = 4                              # 1 per OCPU
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 10_000                   # fd per worker

# Uvicorn legge queste variabili da UvicornWorker
# (equivalente a --loop uvloop --http httptools)
# Nota: in gunicorn la configurazione di uvicorn avviene tramite
# la classe UvicornWorker — non esiste un parametro diretto "loop"
# in gunicorn_conf; usa invece le env var di uvicorn:
import os
os.environ.setdefault("UVICORN_LOOP", "uvloop")
os.environ.setdefault("UVICORN_HTTP", "httptools")

# ─── Bind ─────────────────────────────────────────────────────────────────────

bind = "0.0.0.0:8000"

# ─── Timeout ──────────────────────────────────────────────────────────────────

# 0 = disabilitato — le connessioni SSE sono long-lived per design.
# Gunicorn non deve ucciderle per timeout.
timeout  = 0
graceful_timeout = 30   # secondi per completare le request in corso allo shutdown
keepalive = 5           # HTTP keepalive per /discover

# ─── Logging ──────────────────────────────────────────────────────────────────

accesslog  = "-"   # stdout
errorlog   = "-"   # stderr
loglevel   = "info"

# ─── Preload ──────────────────────────────────────────────────────────────────

# False: ogni worker importa l'app indipendentemente.
# Il lifespan di FastAPI (connect DB, start listener) viene eseguito
# per ogni worker — è il comportamento corretto per asyncpg e asyncio.Event.
# True con asyncio causerebbe problemi con uvloop (stessa issue del fix [3]
# di scheduler.py: Event creato fuori dal loop del worker figlio).
preload_app = False