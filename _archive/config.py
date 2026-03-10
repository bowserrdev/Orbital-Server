import os
import logging
import sys
from pathlib import Path
import logging.handlers
from multiprocessing import Queue

# ==========================================
# CONFIGURAZIONE GENERALE
# ==========================================

# Percorsi
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path("/app/data")
DB_PATH = DATA_DIR / "orbital.db"

# Assicuriamoci che le cartelle esistano
if not DATA_DIR.exists():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

# Configurazione API e Proxy
# Nota: In produzione sarebbe meglio usare os.getenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TVDB_API_KEY = os.getenv("TVDB_API_KEY", "")
SEARCH_CACHE_TTL = int(os.getenv("SEARCH_CACHE_TTL", 60 * 60)) #Default 1h di cache

# Configurazione XML Prowlarr
CAPS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server title="Orbital" />
  <limits max="100" default="50" />
  <registration available="no" open="no" />
  <searching>
    <search available="yes" supportedParams="q" />
    <tv-search available="yes" supportedParams="q,season,ep,imdbid" />
    <movie-search available="yes" supportedParams="q,imdbid" />
  </searching>
  <categories>
    <category id="2000" name="Movies" />
    <category id="5000" name="TV" />
  </categories>
</caps>
"""

# ==========================================
# LOGGING CENTRALIZZATO (MULTIPROCESS SAFE)
# ==========================================

LOG_FILE = DATA_DIR / "orbital.log"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

def get_console_handler():
    """Handler per Docker/Portainer (stdout)"""
    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(processName)-15s | %(name)-10s | %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    return console_handler

def get_file_handler():
    """Handler per file persistente (utile per debug storico)"""
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
    )
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(processName)s:%(name)s] - %(message)s'
    )
    file_handler.setFormatter(formatter)
    return file_handler

def configure_main_logging(log_queue: Queue):
    """
    Configura il processo Main.
    STRATEGIA NON-BLOCCANTE:
    1. Il Main Thread scrive SOLO sulla Queue (veloce, memoria).
    2. Un QueueListener (thread separato) legge la Queue e scrive su Disco/Console (lento).
    """
    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)
    
    # Rimuovi handler esistenti
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    # 1. Prepariamo gli handler "veri" (I/O Bound) per il Listener
    c_handler = get_console_handler()
    f_handler = get_file_handler()
    
    # 2. Avviamo il Listener che smista i messaggi dalla coda agli handler veri
    listener = logging.handlers.QueueListener(log_queue, c_handler, f_handler)
    
    # 3. Al Main Thread diamo SOLO il QueueHandler
    # Così logger.info() diventa non-bloccante
    queue_handler = logging.handlers.QueueHandler(log_queue)
    root.addHandler(queue_handler)
    
    return listener

def configure_worker_logging(log_queue):
    """
    Configura i Worker per inviare tutto alla coda centrale.
    """
    root = logging.getLogger()
    # Importante: nei processi 'spawn', il livello di log viene resettato, reimpostalo
    root.setLevel(LOG_LEVEL) 
    
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    queue_handler = logging.handlers.QueueHandler(log_queue)
    root.addHandler(queue_handler)

# Funzione helper rapida per ottenere logger nei moduli
def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)