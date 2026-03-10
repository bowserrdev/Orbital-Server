import httpx
import asyncio
from lib import logger

class BaseAPI:
    def __init__(self, base_url: str, provider_name: str, session: httpx.AsyncClient):
        # Assicuriamoci che non ci siano slash finali
        self.base_url = base_url.rstrip('/')
        self.provider_name = provider_name
        self.session = session

    async def _request(self, method: str, endpoint: str, **kwargs) -> dict | None:
        """
        Motore centrale. Esegue la chiamata, gestisce i retry e logga gli errori.
        Restituisce un dict (da JSON) o None in caso di fallimento definitivo.
        """
        # Se l'endpoint è un URL completo (es. redirect strani), non usiamo il base_url
        if endpoint.startswith("http"):
            url = endpoint
        else:
            url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        retries = 3
        for attempt in range(retries):
            try:
                response = await self.session.request(method, url, **kwargs)
                
                # --- GESTIONE RATE LIMIT (429) ---
                if response.status_code == 429:
                    # Exponential Backoff: aspetta 1.5s, poi 3.0s, poi 4.5s...
                    wait_time = 1.5 * (attempt + 1)
                    logger.warning(f"⏳ [{self.provider_name}] Rate Limit 429. Attendo {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                
                # --- GESTIONE ERRORI GRAVI (4xx, 5xx) ---
                response.raise_for_status()
                
                # --- SUCCESSO ---
                # Ritorna JSON se presente, altrimenti stringa vuota (per i 204 No Content)
                if not response.content:
                    return None
                return response.json()
                
            except httpx.HTTPStatusError as e:
                # Errori Client/Server (es. 404, 401, 500)
                # Se non è un 429, falliamo subito senza riprovare, non ha senso insistere.
                logger.error(f"❌ [{self.provider_name}] HTTP {e.response.status_code} su {url}")
                return None
                
            except httpx.RequestError as e:
                # Timeout, Rete assente, DNS non trovato
                logger.error(f"🔌 [{self.provider_name}] Errore di rete (Tentativo {attempt+1}/{retries}): {e}")
                await asyncio.sleep(1.0) # Aspettiamo 1 sec prima di riprovare per problemi di rete
                
        # Se esaurisce i retry e non ha fatto 'return', restituisce None
        logger.error(f"☠️ [{self.provider_name}] Fallimento definitivo dopo {retries} tentativi.")
        return None

    # --- HELPER METHODS ---
    async def _get(self, endpoint: str, **kwargs) -> dict | None:
        return await self._request('GET', endpoint, **kwargs)

    async def _post(self, endpoint: str, **kwargs) -> dict | None:
        return await self._request('POST', endpoint, **kwargs)
