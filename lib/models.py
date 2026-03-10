from pydantic import BaseModel, StringConstraints, Field
from typing import Literal, Annotated

IMDb = Field(pattern=r"^tt\d{6,10}$")
TMDb = Field(gt=0, le=99999999)
TVDb = Field(gt=0, le=99999999)
En_Title =Field(default=None, min_length=1, max_length=500)
Year = Field(default=None, ge=1850, le=2999)
LanguageCode = Annotated[str, StringConstraints(min_length=2, max_length=2, pattern=r"^[a-z]{2}$")]
Resolution = Literal[2160, 1080, 720, 576, 480]

# ==========================================
# 1. PROFILI E PUNTEGGI (Configurazione Utente)
# ==========================================

class ScoringWeights(BaseModel):
    """Pesi specifici per le caratteristiche del file"""
    remux: int = 0
    hdr: int = 0
    dv: int = 0
    atmos: int = 0

class MediaProfile(BaseModel):
    """Profilo di configurazione per un singolo tipo di media (movie, show, anime)"""
    max_resolution: Resolution
    preferred_resolution: Resolution | None = None
    max_size_gb: float = 0.0  # 0 = illimitato
    min_score_threshold: int = 0
    preferred_score_threshold: int = 0
    languages: dict[LanguageCode, int] = Field(default_factory=dict)
    weights: ScoringWeights


# ==========================================
# 2. ELEMENTI DA CERCARE (Discriminated Union)
# ==========================================


class MovieItem(BaseModel):
    type: Literal["movie"]
    imdb_id: str = IMDb
    tmdb_id: int = TMDb
    en_title: str | None = En_Title
    year: int | None = Year

class ShowItem(BaseModel):
    type: Literal["show"]
    imdb_id: str = IMDb
    tmdb_id: int = TMDb
    tvdb_id: int = TVDb
    en_title: str | None = En_Title
    year: int | None = Year

class EpisodeItem(BaseModel):
    type: Literal["episode"]
    show_imdb_id: str = IMDb
    show_tmdb_id: int = TMDb
    show_tvdb_id: int = TVDb
    season: int
    episode: int
    ep_imdb_id: str | None = Field(default = None, pattern=r"^tt\d{6,10}$")
    ep_tmdb_id: int | None = Field(default = None, gt=0, le=99999999)
    ep_tvdb_id: int | None = Field(default = None, gt=0, le=99999999)

# "Tipo Magico" moderno: Pydantic sceglie la classe guardando il campo "type"
PlayableItem = Annotated[MovieItem | ShowItem | EpisodeItem, Field(discriminator="type")]


# ==========================================
# 3. RICHIESTE (Kodi -> Server)
# ==========================================

class DiscoverRequest(BaseModel):
    """Payload per l'endpoint /api/v1/discover"""
    language: str = LanguageCode
    debrid_services: list[str] = Field(default_factory=lambda: ["real_debrid"])
    
    # Un dizionario dove la chiave è il tipo ("movie", "show", "anime")
    profiles: dict[str, MediaProfile] 
    
    # La lista mista di film, serie ed episodi che Kodi vuole visualizzare
    items: list[PlayableItem]


class PlayRequest(BaseModel):
    """Payload per l'endpoint /api/v1/play"""
    debrid_services: list[str] = Field(default_factory=lambda: ["real_debrid"])
    profile: MediaProfile
    item: PlayableItem
    exclude_hashes: list[str] = Field(default_factory=list)


# ==========================================
# 4. RISPOSTE (Server -> Kodi)
# ==========================================

class MediaMetadata(BaseModel):
    """Dati pronti per essere salvati nel DB SQLite di Kodi"""
    title: str
    plot: str | None = None
    runtime: int | None = None
    poster_url: str | None = None
    clearlogo_url: str | None = None
    fanart_url: str | None = None

class DiscoverResultItem(BaseModel):
    """Singolo risultato nella risposta di Discovery"""
    imdb_id: str
    status: Literal["available", "processing", "missing"]
    
    # Presenti SOLO se lo status è "available"
    quality_tag: str | None = None
    metadata: MediaMetadata | None = None

class DiscoverResponse(BaseModel):
    """Risposta finale dell'endpoint /api/v1/discover"""
    results: list[DiscoverResultItem]


class PlaySource(BaseModel):
    """Coordinate per lo streaming (dentro PlayResponse)"""
    info_hash: str
    file_ids: str
    torrent_title: str
    file_size: int
    quality_tag: str

class PlayResponse(BaseModel):
    """Risposta finale dell'endpoint /api/v1/play"""
    found: bool
    source: PlaySource | None = None