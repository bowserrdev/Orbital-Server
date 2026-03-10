import httpx

class APIFactory:
    """
    Gestore centralizzato delle connessioni HTTP (Singleton via Module Import).
    """
    def __init__(self):
        self._clients = {}
        
        # Headers di default mascherati
        default_headers = {
            'User-Agent': 'Orbital-Server/1.0 (Linux; Cloud)',
            'Accept': 'application/json'
        }
        
        # Equivalent del tuo HTTPAdapter di Kodi, ma ottimizzato per Async/HTTP2
        self.session = httpx.AsyncClient(
            http2=True, # HTTP/2 riduce massivamente la latenza per le API moderne
            timeout=15.0,
            headers=default_headers,
            limits=httpx.Limits(
                max_keepalive_connections=50, # Quanti socket tenere aperti "a riposo"
                max_connections=100           # Massimo burst assoluto
            )
        )

    def get_tmdb(self):
        if 'tmdb' not in self._clients:
            from api.tmdb_api import TMDBApi
            self._clients['tmdb'] = TMDBApi(session=self.session)
        return self._clients['tmdb']

    def get_tvdb(self):
        if 'tvdb' not in self._clients:
            from api.tvdb_api import TVDBApi
            self._clients['tvdb'] = TVDBApi(session=self.session)
        return self._clients['tvdb']

    async def close_all(self):
        """Da chiamare nel Lifespan di FastAPI durante lo shutdown."""
        await self.session.aclose()
        self._clients.clear()

# L'importazione di questa variabile da altri file garantisce
# automaticamente il pattern Singleton in Python.
api_factory = APIFactory()