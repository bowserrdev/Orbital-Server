from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel
from typing import Optional

@dataclass(slots=True)
class SeriesBlueprint:
    """
    Rappresenta la struttura di una stagione TV (da TMDB).
    Usato per capire se una stagione torrent è completa.
    """
    season_number: int
    episode_count: int
    latest_aired_episode: int
    is_ended: bool
    next_air_date: datetime | None = None

@dataclass(slots=True)
class UserProfile:
    """
    Rappresenta le preferenze di un utente con colonne esplicite.
    Eliminato il config_json per permettere ottimizzazioni nel Worker.
    """
    id: int
    username: str
    max_resolution: int     # Es. 40 (4k), 30 (1080p) - vedi utils.res_rank
    max_size: int           # In Bytes. 0 = illimitato
    
    # Punteggi (Weights)
    score_ita: int
    score_remux: int
    score_hdr: int
    score_dv: int
    score_atmos: int
    score_2160p: int
    score_1080p: int
    score_720p: int
    
    # Soglia minima per considerare un risultato valido per questo utente
    min_score_threshold: int
    preferred_score_threshold: int

@dataclass(slots=True)
class JobContext:
    """
    Contesto passato al Worker per ogni Job.
    Sostituisce i dizionari generici per maggiore chiarezza.
    """
    imdb_id: str
    media_type: str         # 'movie' o 'series'
    titles_map: dict[str, str] # {Titolo: Lingua}
    tmdb_year: int | None
    canonical_title: str
    blueprints: dict[int, SeriesBlueprint] # Key: numero stagione
    
@dataclass(slots=True)
class TorznabParams:
    is_caps: bool
    imdb_id: str | None 
    q : str | None
    media_type: str | None
    season: int | None
    episode: int | None
    limit: int
    offset: int

class UserProfileCreate(BaseModel):
    username: str
    max_resolution: str = "2160p"
    max_size: int = 0
    score_ita: int = 0
    score_remux: int = 0
    score_hdr: int = 0
    score_dv: int = 0
    score_atmos: int = 0
    score_2160p: int = 0
    score_1080p: int = 0
    score_720p: int = 0
    min_score_threshold: int = 0
    preferred_score_threshold: int = 0

class UserProfileUpdate(BaseModel):
    max_resolution: Optional[str] = None
    max_size: Optional[int] = None
    score_ita: Optional[int] = None
    score_remux: Optional[int] = None
    score_hdr: Optional[int] = None
    score_dv: Optional[int] = None
    score_atmos: Optional[int] = None
    score_2160p: Optional[int] = None
    score_1080p: Optional[int] = None
    score_720p: Optional[int] = None
    min_score_threshold: Optional[int] = None
    preferred_score_threshold: Optional[int] = None