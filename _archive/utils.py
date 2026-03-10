import time
from config import get_logger

logger = get_logger("Utils")

def res_rank(res: str) -> int:
    """
    Converte la risoluzione testuale in un punteggio intero per confronti.
    4K/2160p -> 40
    1080p    -> 30
    720p     -> 20
    SD/480p  -> 10
    Unknown  -> 0
    """
    if not res: 
        return 0
    
    res = res.lower()
    if "2160" in res or "4k" in res: 
        return 40
    if "1080" in res: 
        return 30
    if "720" in res: 
        return 20
    if "480" in res or "sd" in res: 
        return 10
    
    return 0

class BlockTimer:
    """
    Context Manager per misurare il tempo di esecuzione di un blocco di codice.
    Uso:
        with BlockTimer("Nome Operazione"):
            ...codice lento...
    """
    def __init__(self, name: str):
        self.name = name
        self.start = 0

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.perf_counter() - self.start
        # Logghiamo a livello INFO così si vede su Portainer
        logger.info(f"⏱️ [TIMING] {self.name}: {elapsed:.4f}s")