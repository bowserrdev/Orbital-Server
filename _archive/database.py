import json
import asyncpg
import os
from config import get_logger

logger = get_logger("Database")

# Configurazione ENV
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "admin_secure_password")
DB_NAME = os.getenv("DB_NAME", "orbital_main")

DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

class DatabaseManager:
    def __init__(self):
        self.pool = None

    async def connect(self):
        """Inizializza il pool AsyncPG verso PostgreSQL 18."""
        try:
            # Setup del pool con parametri ottimizzati per concorrenza
            self.pool = await asyncpg.create_pool(
                dsn=DSN,
                min_size=10,        # Minimo connessioni sempre aperte
                max_size=40,        # Massimo burst per gestire i worker
                command_timeout=60,
                statement_cache_size=1000
            )
            logger.info(f"🔌 Connesso a PostgreSQL 18 ({DB_HOST}/{DB_NAME})")
            await self.init_db()
        except Exception as e:
            logger.critical(f"❌ Errore critico DB: {e}")
            raise e

    async def close(self):
        if self.pool: await self.pool.close()

    async def init_db(self):
        """
        Schema ottimizzato per PG18 con Colonne Generate (STORED).
        """
        async with self.pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

            # 1. Tabella Torrents con Colonne Generate
            # I campi 'gen_*' vengono calcolati automaticamente dal DB all'inserimento.
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS torrents (
                    info_hash TEXT,
                    file_ids TEXT,
                    imdb_id TEXT NOT NULL,
                    title TEXT,
                    size BIGINT,
                    
                    -- DATI GREZZI (JSONB)
                    media_info JSONB NOT NULL DEFAULT '{}'::jsonb,
                    
                    -- DATI SERIE
                    season INTEGER,
                    episodes JSONB,
                    complete BOOLEAN DEFAULT FALSE,
                    
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    -- COLONNE GENERATE AUTOMATICHE (PG18 Optimized)
                    -- Vengono popolate da sole leggendo media_info. 
                    -- STORED significa che sono salvate su disco per velocità di lettura massima.
                    
                    resolution TEXT GENERATED ALWAYS AS (media_info->>'resolution') STORED,
                    hdr BOOLEAN GENERATED ALWAYS AS ((media_info->>'hdr')::boolean) STORED,
                    remux BOOLEAN GENERATED ALWAYS AS ((media_info->>'remux')::boolean) STORED,
                    dv BOOLEAN GENERATED ALWAYS AS ((media_info->>'dolby_vision')::boolean) STORED,
                    audio TEXT GENERATED ALWAYS AS (media_info->>'audio') STORED,
                    languages TEXT GENERATED ALWAYS AS (media_info->>'languages') STORED,

                    PRIMARY KEY (info_hash, file_ids)
                );
            ''')
            
            # 2. Indici B-Tree sulle colonne generate (Velocissimi per lo Scoring)
            # Sostituiscono l'indice GIN generico per questi campi specifici
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_res ON torrents(resolution);')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_hdr ON torrents(hdr);')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_remux ON torrents(remux);')
            
            # Indice GIN resta utile per ricerche full-text o campi non mappati
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_torrents_media_info ON torrents USING GIN (media_info);')
            
            # Indice Lookup standard
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_torrents_lookup ON torrents (imdb_id, season);')

            # ... Creazione altre tabelle (media_metadata, blueprints, profiles) identica a prima ...
            await self._init_aux_tables(conn)


    async def _init_aux_tables(self, conn):

            # 2. Tabella Metadata (Cache TMDB)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS media_metadata (
                    imdb_id TEXT PRIMARY KEY,
                    tmdb_id TEXT,
                    media_type TEXT,
                    year INTEGER,
                    titles_json JSONB,
                    last_searched TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            ''')
            
            # 3. Tabella Blueprint (Struttura Serie TVDB)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS series_blueprint (
                    name TEXT,
                    imdb_id TEXT,
                    season_number INTEGER,
                    episode_count INTEGER,
                    latest_aired_episode INTEGER DEFAULT 0,
                    is_ended BOOLEAN,
                    next_air_date TIMESTAMP,
                    last_searched TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (imdb_id, season_number)
                );
            ''')

            # 4. Profili Utente
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_profiles (
                    id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE,
                    max_resolution TEXT DEFAULT '2160p',
                    max_size BIGINT DEFAULT 0,
                    score_ita INTEGER DEFAULT 0,
                    score_remux INTEGER DEFAULT 0,
                    score_hdr INTEGER DEFAULT 0,
                    score_dv INTEGER DEFAULT 0,
                    score_atmos INTEGER DEFAULT 0,
                    score_2160p INTEGER DEFAULT 0,
                    score_1080p INTEGER DEFAULT 0,
                    score_720p INTEGER DEFAULT 0,
                    min_score_threshold INTEGER DEFAULT 50,
                    preferred_score_threshold INTEGER DEFAULT 1000
                );
            ''')
            
            # Utente Default
            await conn.execute("""
                INSERT INTO user_profiles (username) VALUES ('luca') 
                ON CONFLICT (username) DO NOTHING;
            """)

    async def insert_batch_torrents(self, items: list[dict]):
        if not items: return
        
        records = []
        for item in items:
            media_info = {
                "resolution": item.get('resolution'),
                "quality": item.get('quality'),
                "codec": item.get('codec'),
                "audio": item.get('audio'),
                "languages": item.get('languages'),
                "hdr": bool(item.get('hdr', 0)),
                "remux": bool(item.get('remux', 0)),
                "dolby_vision": bool(item.get('dolby_vision', 0)),
                "arr_title": item.get('arr_title')
            }
            
            records.append((
                item['info_hash'],
                str(item['file_ids']),
                item['imdb_id'],
                item.get('title'),
                item.get('file_size', 0),
                json.dumps(media_info),
                item.get('season'),
                json.dumps(item.get('episodes')) if item.get('episodes') else None,
                bool(item.get('complete', 0))
            ))

        async with self.pool.acquire() as conn:
            try:
                async with conn.transaction():
                    # 1. Creiamo tabella temporanea
                    await conn.execute("""
                        CREATE TEMP TABLE tmp_torrents 
                        (LIKE torrents EXCLUDING GENERATED INCLUDING DEFAULTS) 
                        ON COMMIT DROP;
                    """)
                    
                    # 2. Copiamo i dati grezzi
                    await conn.copy_records_to_table(
                        'tmp_torrents',
                        records=records,
                        columns=['info_hash', 'file_ids', 'imdb_id', 'title', 'size', 'media_info', 'season', 'episodes', 'complete']
                    )
                    
                    # 3. FIX: INSERT ESPLICITO (Escludiamo le colonne gen_*)
                    # Invece di SELECT *, elenchiamo le colonne fisiche.
                    # Postgres calcolerà le colonne generate automaticamente dall'inserimento di media_info.
                    tag = await conn.execute('''
                        INSERT INTO torrents (info_hash, file_ids, imdb_id, title, size, media_info, season, episodes, complete)
                        SELECT info_hash, file_ids, imdb_id, title, size, media_info, season, episodes, complete
                        FROM tmp_torrents
                        ON CONFLICT (info_hash, file_ids) DO NOTHING
                    ''')
                    
                    # logger.info(f"💾 [DB Success] Batch completato: {tag}")

            except Exception as e:
                logger.error(f"❌ Errore Insert Batch: {e}", exc_info=True)

    # --- QUERY INTELLIGENTE (Scoring SQL) ---
    async def get_cached_scored(self, imdb_id: str, profile, season: int = None, episode: int = None):
        """
        Usa le colonne generate (gen_*) per calcolare lo score molto più velocemente.
        """
        
        # Nota come la query è più pulita: usa gen_resolution invece di media_info->>'resolution'
        sql = """
            SELECT 
                info_hash, file_ids, imdb_id, title, size as file_size, media_info, season, episodes, complete,
                (CASE WHEN season IS NOT NULL THEN 'series' ELSE 'movie' END) as media_type,
                (
                    -- Score Risoluzione (Usa B-Tree Index Scan)
                    (CASE WHEN resolution = '2160p' THEN $2 ELSE 0 END) +
                    (CASE WHEN resolution = '1080p' THEN $3 ELSE 0 END) +
                    (CASE WHEN resolution = '720p'  THEN $4 ELSE 0 END) +
                    
                    -- Features (Booleani diretti)
                    (CASE WHEN remux IS TRUE THEN $5 ELSE 0 END) +
                    (CASE WHEN hdr IS TRUE   THEN $6 ELSE 0 END) +
                    (CASE WHEN dv IS TRUE    THEN $7 ELSE 0 END) +
                    
                    -- Audio & Lingua (ILIKE su colonna testo estratta)
                    (CASE WHEN audio ILIKE '%Atmos%' THEN $8 ELSE 0 END) +
                    (CASE WHEN languages ILIKE '%Italian%' THEN $9 ELSE 0 END)

                ) as _score
            FROM torrents
            WHERE imdb_id = $1
        """
        
        args = [
            imdb_id,
            profile.score_2160p, profile.score_1080p, profile.score_720p,
            profile.score_remux, profile.score_hdr, profile.score_dv,
            profile.score_atmos, profile.score_ita
        ]
        
        idx = 10 

        if season is not None:
            sql += f" AND season = ${idx}"
            args.append(season)
            idx += 1
            
            if episode is not None:
                # Qui usiamo ancora JSONB per l'array episodi, è la soluzione migliore per le liste
                sql += f" AND (complete = TRUE OR episodes @> CAST(${idx} as text)::jsonb)"
                args.append(json.dumps([episode]))
                idx += 1

        sql += " ORDER BY _score DESC LIMIT 50"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            results = []
            for r in rows:
                d = dict(r)
                meta = json.loads(d.pop('media_info'))
                d.update(meta)
                if d.get('episodes'): d['episodes'] = json.loads(d['episodes'])
                
                if d['_score'] >= profile.min_score_threshold:
                    results.append(d)
            return results

    # --- METODI ACCESSORI (Rimasti simili, adattati Async) ---
    async def get_metadata(self, imdb_id: str):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM media_metadata WHERE imdb_id=$1", imdb_id)
            if row:
                d = dict(row)
                d['titles'] = json.loads(d['titles_json']) if d.get('titles_json') else {}
                return d
        return None

    async def save_metadata(self, meta: dict):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO media_metadata (imdb_id, tmdb_id, media_type, year, titles_json)
                VALUES ($1, $2, $3, $4, $5::jsonb)
                ON CONFLICT (imdb_id) DO UPDATE SET 
                    tmdb_id=EXCLUDED.tmdb_id, titles_json=EXCLUDED.titles_json, last_updated=CURRENT_TIMESTAMP
            ''', meta['imdb_id'], str(meta.get('tmdb_id', '')), meta['media_type'], meta.get('year'), json.dumps(meta.get('titles', {})))

    async def update_last_searched(self, imdb_id: str):
        """
        Aggiorna il timestamp SOLO se il film esiste già nel DB.
        Se è un film nuovo, non fa nulla (lo creerà save_metadata dopo).
        """
        async with self.pool.acquire() as conn:
            # Ritorna "UPDATE 1" se c'era, "UPDATE 0" se non c'era.
            # Nessun zombie creato.
            await conn.execute("""
                UPDATE media_metadata 
                SET last_searched = CURRENT_TIMESTAMP 
                WHERE imdb_id = $1
            """, imdb_id)

    async def update_series_last_searched(self, imdb_id: str, season: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE series_blueprint 
                SET last_searched = CURRENT_TIMESTAMP
                WHERE imdb_id = $1 AND season_number = $2
            """, imdb_id, season)

    async def get_profiles_infos(self, username: str = None):
        from models import UserProfile
        from utils import res_rank
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM user_profiles")
        
        all_profiles = []
        current_profile = None
        max_res = 0
        max_size = 0
        for r in rows:
            p = UserProfile(
                id=r['id'], username=r['username'], max_resolution=res_rank(r['max_resolution']),
                max_size=r['max_size'], score_ita=r['score_ita'], score_remux=r['score_remux'],
                score_hdr=r['score_hdr'], score_dv=r['score_dv'], score_atmos=r['score_atmos'],
                score_2160p=r['score_2160p'], score_1080p=r['score_1080p'], score_720p=r['score_720p'],
                min_score_threshold=r['min_score_threshold'],
                preferred_score_threshold=r['preferred_score_threshold']
            )
            all_profiles.append(p)
            if username and p.username == username: current_profile = p
            if p.max_resolution>max_res: max_res = p.max_resolution
            if p.max_size>max_size: max_size = p.max_size
        return (current_profile, all_profiles, {'max_res': max_res, 'max_size': max_size}) 

    async def get_series_blueprints(self, imdb_id: str):
        from models import SeriesBlueprint
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM series_blueprint WHERE imdb_id=$1", imdb_id)
        bps = {}
        for r in rows:
            bps[r['season_number']] = SeriesBlueprint(
                season_number=r['season_number'], episode_count=r['episode_count'],
                latest_aired_episode=r['latest_aired_episode'], is_ended=r['is_ended'],
                next_air_date=r['next_air_date']
            )
        return bps

    async def put_series_blueprints(self, name: str, imdb_id: str, blueprints: dict, active_season: int = None):
        if not blueprints: return
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for s in blueprints.values():
                    is_active = (active_season is not None and s.season_number == active_season)
                    await conn.execute('''
                        INSERT INTO series_blueprint 
                        (name, imdb_id, season_number, episode_count, latest_aired_episode, is_ended, next_air_date, last_searched, last_updated)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, 
                                CASE WHEN $8::boolean THEN CURRENT_TIMESTAMP ELSE NULL END,
                                CURRENT_TIMESTAMP)
                        ON CONFLICT (imdb_id, season_number) DO UPDATE SET
                            name = EXCLUDED.name, -- Aggiorniamo il nome se cambia
                            episode_count=EXCLUDED.episode_count,
                            latest_aired_episode=EXCLUDED.latest_aired_episode,
                            next_air_date=EXCLUDED.next_air_date,
                            last_updated=CURRENT_TIMESTAMP,
                            last_searched=COALESCE(EXCLUDED.last_searched, series_blueprint.last_searched)
                     ''', 
                     name, imdb_id, s.season_number, s.episode_count, s.latest_aired_episode, s.is_ended, s.next_air_date, is_active
                     )

    async def get_season_metadata(self, imdb_id: str, season: int):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM series_blueprint WHERE imdb_id=$1 AND season_number=$2", imdb_id, season)
        return dict(row) if row else None
    
    async def reset_last_searched(self, imdb_id: str, season: int | None = None):
         async with self.pool.acquire() as conn:
             if season is None:
                 await conn.execute("UPDATE media_metadata SET last_searched=NULL WHERE imdb_id=$1", imdb_id)
                 await conn.execute("UPDATE series_blueprint SET last_searched=NULL WHERE imdb_id=$1", imdb_id)
             else:
                 await conn.execute("UPDATE series_blueprint SET last_searched=NULL WHERE imdb_id=$1 AND season_number=$2", imdb_id, season)

    async def delete_torrent_by_id(self, torrent_id: int):
        pass # Non implementato per ora su hash-key DB

db_manager = DatabaseManager()