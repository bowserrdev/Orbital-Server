# logger.py
import sys
from loguru import logger

def setup_logger():
    """Inizializza il logger centralizzato per tutta l'app."""
    
    # 1. Rimuove il logger di default per evitare doppioni
    logger.remove()

    # 2. Handler per la Console (Docker/Portainer) - Colorato e pulito
    logger.add(
        sys.stdout,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan> - <level>{message}</level>",
        level="INFO",
        colorize=True
    )

    # 3. Handler per il File (su disco) - Asincrono e con rotazione
    logger.add(
        "/data/orbital.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function} - {message}",
        level="DEBUG",
        rotation="10 MB",     # Quando il file raggiunge 10MB, ne crea uno nuovo
        retention="5 days",   # Cancella i log più vecchi di 5 giorni
        enqueue=True          # FONDAMENTALE: lo rende non-bloccante per l'Event Loop
    )

# Esportiamo direttamente l'oggetto 'logger' configurato
__all__ = ["logger", "setup_logger"]