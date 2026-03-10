from abc import ABC, abstractmethod
import httpx
from base_api import BaseAPI
from models import PlayableItem

class BaseProvider(BaseAPI, ABC):
    """
    Interfaccia standard per tutti i provider Torrent (DMM, Torbox, ecc.).
    Eredita da BaseAPI, quindi possiede già self._get, self._post e self.session.
    """
    
    def __init__(self, name: str, base_url: str, session: httpx.AsyncClient):
        # Inizializziamo il genitore (BaseAPI) passando i dati necessari
        super().__init__(base_url=base_url, provider_name=name, session=session)

    @abstractmethod
    async def search(self, item: PlayableItem, debrid_services: list[str]) -> list[dict]:
        """
        METODO OBBLIGATORIO.
        Deve prendere i dati dell'elemento cercato, fare le chiamate (usando self._get o simili),
        e restituire una lista standardizzata di torrent grezzi estratti.
        
        Args:
            item (PlayableItem): Il film o episodio da cercare (Pydantic model)
            debrid_services (list[str]): I servizi abilitati (es. ["real_debrid"])
            
        Returns:
            list[dict]: Array di torrent (DEVE contenere almeno 'hash' o 'info_hash' e 'title')
        """
        pass