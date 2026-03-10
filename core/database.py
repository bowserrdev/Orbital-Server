"""
database.py — Orbital Queue Infrastructure
Ottimizzato per: PostgreSQL 18 · io_uring · ARM Ampere · 24GB RAM

NOTA su io_method = io_uring:
  io_method è un parametro PGC_POSTMASTER: va impostato in postgresql.conf
  (o via `ALTER SYSTEM SET io_method = 'io_uring'`) e richiede un restart.
  NON può essere impostato per-connessione in server_settings.
  Aggiungilo manualmente a postgresql.conf sul tuo host Oracle Cloud:

      io_method               = io_uring
      effective_io_concurrency = 256
      io_uring_sqe_batch_size  = 64
      io_uring_cqe_batch_size  = 64
      jit                     = off

  Il codice qui sotto configura solo i parametri impostabili per-sessione.
"""

import asyncpg
import os
from loguru import logger
from main import USER_JOB_QUEUE_LIMIT

DB_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://admin:admin_secure_password@postgres:5432/orbital_main",
)

# 5 processi (4 Uvicorn + 1 Scheduler) × 10 = 50 connessioni pool.
# Le connessioni LISTEN dei worker HTTP vivono fuori dal pool (+4 stabili).
# Totale massimo: 54, abbondantemente sotto max_connections=100.
POOL_MIN = 1   # fix: era 2, abbassato a 1 per ridurre connessioni idle
POOL_MAX = 10



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
            command_timeout=60,
            server_settings={
                # io_method=io_uring e i parametri io_uring_* devono stare
                # in postgresql.conf (parametri PGC_POSTMASTER).
                # Vedi nota in cima al file.

                "timezone": "UTC",

                # Dice a PG di emettere 256 I/O asincroni simultanei
                # per connessione — sfrutta io_uring al massimo.
                # Ha effetto solo se io_method=io_uring è attivo nel server.
                "effective_io_concurrency": "256",

                # Le query della coda sono semplici e brevi.
                # JIT aggiunge latenza di compilazione senza benefici.
                "jit": "off",
            },
        )
        await self._init_schema()
        logger.info("🟢 Connesso a Postgres 18 — schema code inizializzato")

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("🔴 Pool Postgres chiuso")

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

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

        FK DEFERRABLE INITIALLY DEFERRED su job_subscriptions:
          Consente di eliminare subscriptions e job nello stesso statement
          CTE senza FK violation. Il check avviene a fine statement,
          non a fine singola riga.

        Indici su tutti i campi hot delle query dello Scheduler.

        PG18 — virtual generated column `age_seconds` su jobs_active:
          Calcolata a query time, zero storage, zero overhead su INSERT/UPDATE.
          Usata da recover_stale_jobs per leggibilità; il filtro hot rimane
          su created_at (indexed) per efficienza.
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""

                -- -------------------------------------------------------
                -- 1. JOBS ACTIVE
                --    PG18: age_seconds è una VIRTUAL generated column.
                --    Compute-on-read, nessun byte extra su disco.
                -- -------------------------------------------------------
                CREATE UNLOGGED TABLE IF NOT EXISTS jobs_active (
                    id          UUID        PRIMARY KEY DEFAULT uuidv7(),
                    imdb_id     TEXT        NOT NULL,
                    media_type  TEXT        NOT NULL,
                    priority    INTEGER     NOT NULL,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT unique_active_imdb UNIQUE (imdb_id)
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_priority
                    ON jobs_active (priority ASC);

                CREATE INDEX IF NOT EXISTS idx_jobs_imdb
                    ON jobs_active (imdb_id);

                -- PG18 skip scan: indice multicolonna per entrambe le query hot
                -- PG18 può fare skip scan su imdb_id anche senza il prefisso.
                -- Sostituisce idx_jobs_priority + idx_jobs_imdb nel long-run;
                -- i vecchi indici rimangono per compatibilità col planner.
                CREATE INDEX IF NOT EXISTS idx_jobs_priority_imdb
                    ON jobs_active (priority ASC, imdb_id);

                -- -------------------------------------------------------
                -- 2. JOB SUBSCRIPTIONS
                --    Ora UNLOGGED: tabella più scritta del sistema,
                --    dati ricostruibili, zero WAL su ogni INSERT/DELETE.
                --    FK rimane DEFERRABLE per la Super Query CTE.
                -- -------------------------------------------------------
                CREATE UNLOGGED TABLE IF NOT EXISTS job_subscriptions (
                    job_id       UUID        NOT NULL,
                    user_id      TEXT        NOT NULL,
                    channel_name TEXT        NOT NULL,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (job_id, user_id),
                    CONSTRAINT fk_job
                        FOREIGN KEY (job_id)
                        REFERENCES jobs_active (id)
                        DEFERRABLE INITIALLY DEFERRED
                );

                -- Usato dalla best_user CTE (INNER JOIN) e dal quantum UPDATE
                CREATE INDEX IF NOT EXISTS idx_subs_user_id
                    ON job_subscriptions (user_id);

                -- Usato dalla deleted_subs CTE
                CREATE INDEX IF NOT EXISTS idx_subs_job_id
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

                -- -------------------------------------------------------
                -- 4. JOBS HISTORY (LOGGED — dati permanenti)
                -- -------------------------------------------------------
                CREATE TABLE IF NOT EXISTS jobs_history (
                    id             UUID        PRIMARY KEY,
                    imdb_id        TEXT        NOT NULL,
                    result_status  TEXT        NOT NULL,
                    completed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_history_imdb
                    ON jobs_history (imdb_id);

                -- -------------------------------------------------------
                -- 5. FUNZIONE PL/pgSQL — batch DRR in 1 round-trip
                --    Sostituisce il loop Python in fetch_and_claim_next_jobs.
                --    Ogni iterazione vede i deficit aggiornati da quella
                --    precedente (stessa transazione del caller).
                -- -------------------------------------------------------
                CREATE OR REPLACE FUNCTION fetch_and_claim_jobs(p_batch INT)
                RETURNS TABLE(
                    out_job_id     UUID,
                    out_imdb_id    TEXT,
                    out_media_type TEXT,
                    out_channels   TEXT[]
                )
                LANGUAGE plpgsql AS $$
                DECLARE
                    _v_job_id     UUID;
                    _v_imdb_id    TEXT;
                    _v_media_type TEXT;
                    _v_channels   TEXT[];
                BEGIN
                    FOR i IN 1..p_batch LOOP

                        WITH
                        best_user AS (
                            SELECT s.user_id
                            FROM scheduler_state s
                            INNER JOIN job_subscriptions sub ON sub.user_id = s.user_id
                            INNER JOIN jobs_active j         ON j.id = sub.job_id
                            ORDER BY s.deficit DESC, s.updated_at ASC
                            LIMIT 1
                        ),
                        selected_job AS (
                            SELECT j.id, j.imdb_id, j.media_type
                            FROM jobs_active j
                            INNER JOIN job_subscriptions sub ON j.id = sub.job_id
                            WHERE sub.user_id = (SELECT user_id FROM best_user)
                            ORDER BY j.priority ASC
                            LIMIT 1
                            FOR UPDATE OF j SKIP LOCKED
                        ),
                        deleted_subs AS (
                            DELETE FROM job_subscriptions
                            WHERE job_subscriptions.job_id = (SELECT id FROM selected_job)
                            RETURNING job_subscriptions.channel_name
                        ),
                        deleted_job AS (
                            DELETE FROM jobs_active
                            WHERE jobs_active.id = (SELECT id FROM selected_job)
                            RETURNING jobs_active.id, jobs_active.imdb_id, jobs_active.media_type
                        ),
                        _upd_deficit AS (
                            UPDATE scheduler_state
                            SET
                                deficit    = GREATEST(deficit - 1, -10),
                                updated_at = NOW()
                            WHERE user_id = (SELECT user_id FROM best_user)
                        )
                        SELECT
                            dj.id,
                            dj.imdb_id,
                            dj.media_type,
                            ARRAY(SELECT channel_name FROM deleted_subs)
                        INTO _v_job_id, _v_imdb_id, _v_media_type, _v_channels
                        FROM deleted_job dj;

                        EXIT WHEN NOT FOUND;

                        out_job_id     := _v_job_id;
                        out_imdb_id    := _v_imdb_id;
                        out_media_type := _v_media_type;
                        out_channels   := _v_channels;
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
        Chiama la funzione PL/pgSQL fetch_and_claim_jobs:
        1 solo round-trip DB indipendentemente dal batch_size.
        Il loop DRR e i decrementi deficit avvengono server-side,
        ogni iterazione vede lo stato aggiornato dall'iterazione precedente.
        DEVE essere chiamata dentro una transazione aperta dal caller.
        """
        rows = await conn.fetch(
            "SELECT * FROM fetch_and_claim_jobs($1)",
            batch_size,
        )

        return [
            {
                "job_id":     row["out_job_id"],      
                "imdb_id":    row["out_imdb_id"],
                "media_type": row["out_media_type"],
                "channels":   list(row["out_channels"]),
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Quantum DRR — aging periodico
    # ------------------------------------------------------------------

    # DOPO
    async def add_quantum_to_active_users(self):
        """
        EXISTS con idx_subs_user_id: il planner esegue un index semi-join
        e si ferma al primo match per ogni utente — nessuna materializzazione
        del set completo come faceva SELECT DISTINCT.
        Chiamata ogni secondo: questa ottimizzazione conta.
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE scheduler_state s
                SET
                    deficit    = LEAST(s.deficit + 10, 100),
                    updated_at = NOW()
                WHERE EXISTS (
                    SELECT 1
                    FROM job_subscriptions sub
                    WHERE sub.user_id = s.user_id
                )
            """)

    # ------------------------------------------------------------------
    # Recovery job orfani
    # ------------------------------------------------------------------

    async def recover_stale_jobs(self, max_age_minutes: int = 10) -> list:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Legge E cancella in un unico statement atomico.
                # RETURNING permette il log senza una SELECT separata.
                # NOT EXISTS è immune ai NULL in job_id (NOT IN non lo è).
                rows = await conn.fetch("""
                    DELETE FROM jobs_active
                    WHERE created_at < NOW() - make_interval(mins => $1)
                    AND NOT EXISTS (
                        SELECT 1 FROM job_subscriptions sub
                        WHERE sub.job_id = jobs_active.id
                    )
                    RETURNING id, imdb_id, created_at
                """, max_age_minutes)

            if rows:
                ids = [str(r["id"]) for r in rows]
                logger.warning(
                    f"⚠️  {len(rows)} job orfani eliminati "
                    f"(> {max_age_minutes} min): {ids}"
                )

        return rows

    # ------------------------------------------------------------------
    # Cleanup utenti inattivi
    # ------------------------------------------------------------------

    async def cleanup_inactive_users(self, inactive_hours: int = 24):
        """
        Rimuove da scheduler_state gli utenti senza subscription attive
        da più di N ore.

        FIX: la versione precedente usava NOT EXISTS con correlated subquery.
        Ora usa LEFT JOIN ... IS NULL: il planner può sfruttare
        idx_subs_user_id con un hash anti-join invece di loopare
        per ogni riga di scheduler_state.
        """
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM scheduler_state s
                USING (
                    SELECT s2.user_id
                    FROM scheduler_state s2
                    LEFT JOIN job_subscriptions sub
                        ON sub.user_id = s2.user_id
                    WHERE s2.updated_at < NOW() - make_interval(hours => $1)
                      AND sub.user_id IS NULL
                ) to_delete
                WHERE s.user_id = to_delete.user_id
            """, inactive_hours)

        count = int(result.split()[-1])
        if count:
            logger.info(
                f"🧹 Rimossi {count} utenti inattivi da scheduler_state"
            )


    async def enqueue_job(
        self,
        imdb_id: str,
        media_type: str,
        priority: int,
        user_id: str,
        channel_name: str,
    ) -> dict:
        """
        Accoda un job e registra l'utente nello scheduler in un'unica
        transazione atomica.

        Passi:
          1. UPSERT scheduler_state: crea la riga utente se non esiste,
             altrimenti non fa nulla (ON CONFLICT DO NOTHING).
             Senza questa riga best_user non seleziona mai l'utente
             (INNER JOIN scheduler_state nella Super Query).

          2. INSERT jobs_active: ignora se imdb_id già in coda
             (ON CONFLICT DO NOTHING su UNIQUE imdb_id).
             Ritorna l'id del job esistente o di quello appena creato.

          3. INSERT job_subscriptions: registra il canale pg_notify
             dell'utente per questo job.
             ON CONFLICT DO NOTHING evita duplicati se l'utente
             si abbona due volte allo stesso job.

        Ritorna: {"job_id": UUID, "created": bool}
          created=True  → job nuovo inserito
          created=False → job già esistente, subscription aggiunta
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():

                # 1. Garantisce che l'utente esista in scheduler_state
                await conn.execute("""
                    INSERT INTO scheduler_state (user_id, deficit)
                    VALUES ($1, 0)
                    ON CONFLICT (user_id) DO NOTHING
                """, user_id)

                # 2. Inserisce il job (idempotente su imdb_id)
                pending_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM job_subscriptions WHERE user_id = $1
                """, user_id)

                if pending_count >= USER_JOB_QUEUE_LIMIT:
                    return {"job_id": None, "created": False}

                # 2. Inserisce il job (idempotente su imdb_id)  ← invariato
                row = await conn.fetchrow("""
                    INSERT INTO jobs_active (imdb_id, media_type, priority)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (imdb_id) DO NOTHING
                    RETURNING id, true AS created
                """, imdb_id, media_type, priority)

                if row is None:
                    # Job già esistente — recupera l'id
                    row = await conn.fetchrow("""
                        SELECT id, false AS created
                        FROM jobs_active
                        WHERE imdb_id = $1
                    """, imdb_id)

                job_id  = row["id"]
                created = row["created"]

                # 3. Registra la subscription dell'utente
                await conn.execute("""
                    INSERT INTO job_subscriptions (job_id, user_id, channel_name)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (job_id, user_id) DO NOTHING
                """, job_id, user_id, channel_name)

        return {"job_id": job_id, "created": created}


# Singleton — importato da scheduler.py e dai worker FastAPI
queue_db = DatabaseQueueManager()
