import asyncpg
import os
from logger import logger

# Leggiamo le variabili d'ambiente (con fallback per sviluppo locale)
DB_USER = os.getenv("POSTGRES_USER", "orbital_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "orbital_pass")
DB_NAME = os.getenv("POSTGRES_DB", "orbital_main")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres") 

# Costruiamo la DSN (Data Source Name)
DB_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

POOL_MIN = int(os.getenv("DB_POOL_MIN", 1))
POOL_MAX = int(os.getenv("DB_POOL_MAX", 10))

USER_JOB_QUEUE_LIMIT = 50

class DatabaseQueueManager:

    def __init__(self):
        self.pool: asyncpg.Pool | None = None

    # ------------------------------------------------------------------
    # Connessione
    # ------------------------------------------------------------------

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            dsn=DB_DSN,
            min_size=POOL_MIN,
            max_size=POOL_MAX,
            command_timeout=60
        )
        await self._init_schema()
        logger.info("Connesso a Postgres 18 — schema code inizializzato")

    async def connect_listener(self) -> asyncpg.Connection:
        """
        Crea una connessione dedicata fuori dal pool per LISTEN/NOTIFY.
        Deve essere chiusa manualmente dal chiamante.
        """
        # Nota: Non usiamo self.pool.acquire() perché questa connessione
        # deve rimanere aperta per sempre (o finché non cade).
        # Il pool è fatto per connessioni brevi (query -> rilascio).
        return await asyncpg.connect(dsn=DB_DSN)

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("Pool Postgres chiuso")

    async def _init_schema(self):
        """
        Crea le tabelle se non esistono. Idempotente.

        Scelte architetturali PG18:

        UNLOGGED su jobs_active, job_subscriptions, scheduler_state:
          Tutti e tre contengono dati ricostruibili. UNLOGGED elimina
          il WAL write su ogni INSERT/UPDATE/DELETE — il carico di scrittura
          più intenso del sistema. PG18 migliora il recovery post-crash
          delle UNLOGGED tables. jobs_history rimane LOGGED (dati permanenti).

        uuidv7() su tutte le PK:
          UUID timestamp-ordered nativo PG18. I nuovi record vengono
          sempre inseriti in append sull'indice B-tree (nessuna
          frammentazione, meno lavoro per autovacuum).

        Indici su tutti i campi hot delle query dello Scheduler.
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""

                -- -------------------------------------------------------
                -- 1. JOBS ACTIVE
                --    Compute-on-read, nessun byte extra su disco.
                --    È la tabella dove finiscono tutte le richieste che richiedono scraping
                -- -------------------------------------------------------
                CREATE UNLOGGED TABLE IF NOT EXISTS jobs_active (
                    id          UUID        PRIMARY KEY DEFAULT uuidv7(),
                    
                    -- Dati del Lavoro
                    imdb_id     TEXT        NOT NULL,
                    media_type  TEXT        NOT NULL,
                    tmdb_id     INTEGER,
                    tvdb_id     INTEGER,
                    season      INTEGER,
                    episode     INTEGER,
                    ep_imdb_id  TEXT,
                    ep_tmdb_id  INTEGER,
                    ep_tvdb_id  INTEGER,
                               
                    debrid_services JSONB   NOT NULL DEFAULT '[]'::jsonb,

                    -- Metadati Coda & Stato
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    
                    -- STATO: Se NULL = Pending. Se Valorizzato = Processing.
                    claimed_at  TIMESTAMPTZ, 
                    
                    -- Unicità (su (imdb_id, season, episode))
                    CONSTRAINT unique_job UNIQUE NULLS NOT DISTINCT (imdb_id, season, episode)
                );
                
                -- Indice per lo scheduler che ordina per created_at i record 
                CREATE INDEX IF NOT EXISTS idx_jobs_pending 
                    ON jobs_active (created_at ASC)
                    WHERE claimed_at IS NULL;

                -- INDICE 2: Per il Recovery (Cerca solo i Processing vecchi)    
                CREATE INDEX IF NOT EXISTS idx_jobs_processing 
                    ON jobs_active (claimed_at ASC)
                    WHERE claimed_at IS NOT NULL;
                               
                -- -------------------------------------------------------
                -- 2. JOBS HISTORY (LOGGED — dati permanenti)
                --    Utile per statistiche
                -- -------------------------------------------------------
                CREATE TABLE IF NOT EXISTS jobs_history (
                    id             UUID        PRIMARY KEY,    -- Stesso ID del job originale
                    imdb_id        TEXT        NOT NULL,
                    
                    -- Metadati Temporali Completi (Analisi Performance)
                    created_at     TIMESTAMPTZ NOT NULL,       -- Quando è entrato in coda
                    claimed_at     TIMESTAMPTZ NOT NULL,       -- Quando è iniziato il lavoro
                    completed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- Quando è finito
                    
                    -- Risultato
                    result_status  TEXT        NOT NULL,       -- 'success', 'failed', 'empty'
                    items_found    INTEGER     DEFAULT 0       -- Quanti torrent trovati (opzionale ma utile)
                );

                -- Indice per cache lookup veloce ("Ho già fatto Inception?")
                CREATE INDEX IF NOT EXISTS idx_history_imdb 
                    ON jobs_history (imdb_id);

                -- Indice per pulizia log vecchi ("Cancella roba vecchia di 1 mese")
                CREATE INDEX IF NOT EXISTS idx_history_completed
                    ON jobs_history (completed_at);
                               
                -- -------------------------------------------------------
                -- 3. JOB SUBSCRIPTIONS
                --    Ora UNLOGGED: tabella più scritta del sistema,
                --    dati ricostruibili, zero WAL su ogni INSERT/DELETE.
                --    FK rimane DEFERRABLE per la Super Query CTE.
                -- -------------------------------------------------------
                CREATE UNLOGGED TABLE IF NOT EXISTS job_subscriptions (
                    job_id       UUID        NOT NULL,
                    user_id      TEXT        NOT NULL,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (job_id, user_id),
                    CONSTRAINT fk_job
                        FOREIGN KEY (job_id)
                        REFERENCES jobs_active (id)
                );

                -- Usato dalla best_user CTE (INNER JOIN) e dal quantum UPDATE
                CREATE INDEX IF NOT EXISTS idx_subs_user
                    ON job_subscriptions (user_id);

                -- Usato dalla deleted_subs CTE
                CREATE INDEX IF NOT EXISTS idx_subs_job
                    ON job_subscriptions (job_id);
                               

                -- -------------------------------------------------------
                -- 3. SCHEDULER STATE
                -- -------------------------------------------------------
                CREATE UNLOGGED TABLE IF NOT EXISTS scheduler_state (
                    user_id    TEXT    PRIMARY KEY,
                    deficit    INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                -- Ordinamento DRR hot path (ORDER BY deficit DESC)
                CREATE INDEX IF NOT EXISTS idx_sched_deficit
                    ON scheduler_state (deficit DESC);
                               

                CREATE OR REPLACE FUNCTION fetch_and_claim_jobs(p_batch INT)
                    RETURNS TABLE(
                        out_id         UUID,
                        out_imdb_id    TEXT,
                        out_media_type TEXT,
                        out_tmdb_id    INTEGER,
                        out_tvdb_id    INTEGER,
                        out_season     INTEGER,
                        out_episode    INTEGER,
                        out_ep_imdb_id      TEXT,
                        out_ep_tmdb_id      INTEGER,
                        out_ep_tvdb_id      INTEGER,
                        out_debrid_services JSONB
                    )
                    LANGUAGE plpgsql AS $$
                    DECLARE
                        _v_user_id    TEXT;
                        _v_job_id     UUID;
                        _r_job        RECORD;
                        _v_cost       INTEGER;
                    BEGIN
                        FOR i IN 1..p_batch LOOP

                            -- ── 1. BEST USER (DRR) ────────────────────────────────────────────
                            -- EXISTS con idx_subs_user: semi-join, si ferma al primo match per
                            -- utente — non materializza il set completo come farebbe un JOIN.
                            -- FOR UPDATE OF s SKIP LOCKED: blocca la riga scheduler_state
                            -- dell'utente scelto. Worker concorrenti saltano utenti già presi,
                            -- evitando che due worker claimino job per lo stesso utente in
                            -- parallelo e rompano il conteggio DRR.
                            -- Tiebreak updated_at ASC: a parità di deficit vince chi non riceve
                            -- slot da più tempo — fairness deterministica e auditabile.
                            SELECT s.user_id INTO _v_user_id
                            FROM scheduler_state s
                            WHERE EXISTS (
                                SELECT 1
                                FROM job_subscriptions sub
                                JOIN jobs_active j ON j.id = sub.job_id
                                WHERE sub.user_id = s.user_id
                                AND j.claimed_at IS NULL
                            )
                            ORDER BY s.deficit DESC, s.updated_at ASC
                            LIMIT 1
                            FOR UPDATE OF s SKIP LOCKED;

                            -- Coda globale vuota o tutti gli utenti già lockata da altri worker.
                            EXIT WHEN NOT FOUND;

                            -- ── 2. TARGET JOB ─────────────────────────────────────────────────
                            -- idx_jobs_pending copre (created_at ASC) WHERE
                            -- claimed_at IS NULL — index-only scan sul partial index.
                            -- FOR UPDATE OF j SKIP LOCKED: blocca il job contro claim concorrenti
                            -- (possibile se due worker selezionano utenti diversi con job condivisi,
                            -- anche se raro con subscription per-utente).
                            SELECT j.id INTO _v_job_id
                            FROM jobs_active j
                            JOIN job_subscriptions sub ON j.id = sub.job_id
                            WHERE sub.user_id = _v_user_id
                            AND j.claimed_at IS NULL
                            ORDER BY j.created_at ASC
                            LIMIT 1
                            FOR UPDATE OF j SKIP LOCKED;

                            -- Race window minima: l'utente aveva job pending al passo 1 ma
                            -- un altro worker li ha già claimati tutti prima che arrivassimo qui.
                            -- CONTINUE (non EXIT): spreca un'iterazione del batch ma non
                            -- interrompe il loop — il prossimo ciclo proverà con un altro utente.
                            CONTINUE WHEN NOT FOUND;

                            -- ── 3. CLAIM ──────────────────────────────────────────────────────
                            -- La riga è già lockata dal FOR UPDATE al passo 2.
                            -- SET claimed_at = NOW(): il partial index idx_jobs_pending esclude
                            -- automaticamente questa riga dal prossimo best_user EXISTS check.
                            UPDATE jobs_active
                            SET claimed_at = NOW()
                            WHERE id = _v_job_id
                            RETURNING id, imdb_id, media_type, tmdb_id, tvdb_id, season, episode
                            INTO _r_job;

                            -- ── 4. COSTO DINAMICO ─────────────────────────────────────────────
                            -- Il costo riflette il lavoro reale dello scraper, non l'urgenza:
                            --   movie=10:   1 file, titolo ben formattato, nessuna chiamata TVDB
                            --   episode=20: ricerca stagione + listing file interni per torrent +
                            --               regex su ogni filename + gestione multi-season pack
                            --   show=80:    stima ~4 stagioni medie × 20, pagamento anticipato
                            --               necessario per mantenere l'invariante DRR (il deficit
                            --               deve essere aggiornato prima del prossimo ciclo di
                            --               selezione — non si può aspettare il completamento reale)
                            _v_cost := CASE _r_job.media_type
                                WHEN 'movie' THEN 10
                                WHEN 'show'  THEN 80
                                ELSE              20   -- episode (default esplicito)
                            END;

                            -- ── 5. PAYMENT ────────────────────────────────────────────────────
                            -- Eseguito DOPO EXIT/CONTINUE: garantisce che il deficit venga
                            -- decrementato solo se un job è stato effettivamente claimato.
                            -- GREATEST(..., -500): cap al debito. Senza cap, un utente che
                            -- chiede 20 serie accumula deficit=-1600 e rimane penalizzato per
                            -- ore dopo che la sua coda si svuota, anche se gli altri utenti
                            -- hanno finito i loro job. Il cap bilancia equità e recovery.
                            UPDATE scheduler_state
                            SET deficit    = GREATEST(deficit - _v_cost, -500),
                                updated_at = NOW()
                            WHERE user_id = _v_user_id;

                            -- ── 6. OUTPUT ─────────────────────────────────────────────────────
                            out_id              := _r_job.id;
                            out_imdb_id         := _r_job.imdb_id;
                            out_media_type      := _r_job.media_type;
                            out_tmdb_id         := _r_job.tmdb_id;
                            out_tvdb_id         := _r_job.tvdb_id;
                            out_season          := _r_job.season;
                            out_episode         := _r_job.episode;
                            out_ep_imdb_id      := _r_job.ep_imdb_id;
                            out_ep_tmdb_id      := _r_job.ep_tmdb_id;
                            out_ep_tvdb_id      := _r_job.ep_tvdb_id;
                            out_debrid_services := _r_job.debrid_services;

                            RETURN NEXT;

                        END LOOP;
                    END;
                    $$;
                
            """)
    
    # ------------------------------------------------------------------
    # Super Query DRR — estrazione batch
    # ------------------------------------------------------------------
    async def fetch_and_claim_next_jobs(
        self,
        conn: asyncpg.Connection,
        batch_size: int = 1,
    ) -> list[dict]:
        """
        Chiama la funzione PL/pgSQL fetch_and_claim_jobs.
        Restituisce i job assegnati e marcati come 'claimed' (PROCESSING).
        """
        # Esegue la funzione DB (che ritorna una tabella virtuale)
        rows = await conn.fetch(
            "SELECT * FROM fetch_and_claim_jobs($1)",
            batch_size,
        )

        # Mappa i risultati della funzione PL/pgSQL in un dizionario Python pulito
        return [
            {
                "job_id":     row["out_id"],          # UUID del job
                "imdb_id":    row["out_imdb_id"],
                "media_type": row["out_media_type"],
                "tmdb_id":    row["out_tmdb_id"],     # Utile per API
                "tvdb_id":    row["out_tvdb_id"],     # Utile per API
                "season":     row["out_season"],      # Solo per episodi
                "episode":    row["out_episode"],     # Solo per episodi
            }
            for row in rows
        ]
    

    # ------------------------------------------------------------------
    # Quantum DRR — aging periodico
    # ------------------------------------------------------------------

    async def add_quantum_to_active_users(self):
        """
        È la funzione che aggiunge credito agli utenti ogni secondo. 
        EXISTS con idx_subs_user: il planner esegue un index semi-join
        e si ferma al primo match per ogni utente — nessuna materializzazione
        del set completo come faceva SELECT DISTINCT.
        Chiamata ogni secondo: questa ottimizzazione conta.
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE scheduler_state s
                SET
                    deficit    = LEAST(s.deficit + 30, 100),
                    updated_at = NOW()
                WHERE EXISTS (
                    SELECT 1
                    FROM job_subscriptions sub
                    WHERE sub.user_id = s.user_id
                )
            """)

    # ------------------------------------------------------------------
    # Enqueue Job — Aggiunge il job e l'user nel db
    # ------------------------------------------------------------------

    async def enqueue_job(
        self,
        user_id: str,
        item: dict,
        debrid_services: list[str]
    ) -> dict:
        
        # 1. NORMALIZZAZIONE DATI
        media_type = item["type"]
        
        if media_type == "episode":
            imdb_id = item["show_imdb_id"]
            tmdb_id = item["show_tmdb_id"]
            tvdb_id = item["show_tvdb_id"]
            ep_imdb_id = item.get("ep_imdb_id")
            ep_tmdb_id = item.get("ep_tmdb_id")
            ep_tvdb_id = item.get("ep_tvdb_id")
        else:
            imdb_id = item["imdb_id"]
            tmdb_id = item["tmdb_id"]
            tvdb_id = item.get("tvdb_id")
            ep_imdb_id, ep_tmdb_id, ep_tvdb_id = None, None, None

        season  = item.get("season")
        episode = item.get("episode")
        

        async with self.pool.acquire() as conn:
            
            # Controllo quota FUORI dalla transazione principale.
            pending_count = await conn.fetchval("""
                SELECT COUNT(*) FROM job_subscriptions WHERE user_id = $1
            """, user_id)

            if pending_count >= USER_JOB_QUEUE_LIMIT:
                return {"job_id": None, "created": False}

            # Inizio transazione di scrittura
            async with conn.transaction():
                
                # A. Crea Utente se non esiste
                await conn.execute("""
                    INSERT INTO scheduler_state (user_id, deficit)
                    VALUES ($1, 0)
                    ON CONFLICT (user_id) DO NOTHING
                """, user_id)

                # B. Inserimento Job Principale
                row = await conn.fetchrow("""
                    INSERT INTO jobs_active (
                        imdb_id, media_type, tmdb_id, tvdb_id, 
                        season, episode, ep_imdb_id, ep_tmdb_id, ep_tvdb_id,
                        debrid_services
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
                    ON CONFLICT ON CONSTRAINT unique_job DO NOTHING
                    RETURNING id, true AS created
                """, imdb_id, media_type, tmdb_id, tvdb_id, 
                     season, episode, ep_imdb_id, ep_tmdb_id, ep_tvdb_id,
                     debrid_services)

                if row is None:
                    # C. Job Esistente: Recuperiamo l'ID
                    row = await conn.fetchrow("""
                        SELECT id, false AS created
                        FROM jobs_active
                        WHERE imdb_id = $1 
                          AND season IS NOT DISTINCT FROM $2 
                          AND episode IS NOT DISTINCT FROM $3
                    """, imdb_id, season, episode)

                    if row is None:
                        # Job appena terminato e cancellato dalla coda prima del nostro fetch
                        return {"job_id": None, "created": False}
                    
                    job_id = row["id"]
                    
                    # D. UNION dei servizi Debrid
                    # NOTA (Claude): Se il job è già in stato claimed_at IS NOT NULL, 
                    # questo UPDATE non ha effetto sullo scraping in corso — 
                    # i nuovi servizi saranno usati solo in caso di re-queue 
                    # dopo un eventuale failure.
                    await conn.execute("""
                        UPDATE jobs_active
                        SET debrid_services = (
                            SELECT jsonb_agg(DISTINCT svc)
                            FROM jsonb_array_elements_text(
                                debrid_services || $1::jsonb
                            ) AS svc
                        )
                        WHERE id = $2
                    """, debrid_services, job_id)
                else:
                    job_id = row["id"]

                created = row["created"]

                # E. Iscrizione dell'utente al Job
                await conn.execute("""
                    INSERT INTO job_subscriptions (job_id, user_id)
                    VALUES ($1, $2)
                    ON CONFLICT (job_id, user_id) DO NOTHING
                """, job_id, user_id)

        return {"job_id": job_id, "created": created}

queue_db = DatabaseQueueManager()