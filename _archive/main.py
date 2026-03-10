import asyncio
import time
import signal
import multiprocessing
from typing import Optional
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from concurrent.futures import ProcessPoolExecutor
from fastapi import FastAPI, Request, Response
from fastapi.responses import RedirectResponse
from fastapi.openapi.docs import get_swagger_ui_html
import uvicorn

# Imports locali

from config import configure_main_logging, configure_worker_logging, get_logger, CAPS_XML, TVDB_API_KEY, SEARCH_CACHE_TTL
from database import db_manager
from models import JobContext, TorznabParams, UserProfileCreate, UserProfileUpdate
from services import MetadataService, SearchService
from dmmapi import DMMApi
import processing

logger = get_logger("Main")

# ==========================================
# CONFIGURAZIONE GLOBALE
# ==========================================
FAST_LANE_THRESHOLD = 3
RESULTS_NUM = 5
MAX_WORKERS = 3
MAX_PAGES = 3

# Variabili Globali gestite dal Lifespan
executor = None
dmm_api = None
db_queue = None
db_thread = None
log_listener = None

# ==========================================
# WORKERS
# ==========================================
def _init_worker(log_queue):
    """
    Eseguita all'avvio di ogni processo worker.
    1. Ignora il CTRL+C (SIGINT) così non crashano stampando traceback.
    2. Configura il logging.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    configure_worker_logging(log_queue)

async def db_writer_worker(queue: asyncio.Queue):
    """
    Consumer Async che scrive batch di torrent su PostgreSQL.
    Sostituisce il vecchio DbSaveThread.
    """
    logger.info("💾 DB Writer Task avviato (AsyncPG).")
    while True:
        # Preleva batch dalla coda
        batch = await queue.get()
        
        # Poison Pill per chiusura pulita
        if batch is None:
            queue.task_done()
            break
        
        try:
            # Scrittura Async su Postgres
            await db_manager.insert_batch_torrents(batch)
            logger.debug(f"✅ [DB] Scritti {len(batch)} record.")
        except Exception as e:
            logger.error(f"❌ Errore scrittura DB batch: {e}")
        finally:
            queue.task_done()

# ==========================================
# LIFESPAN (Startup/Shutdown)
# ==========================================

def _executor_warmup():
    """Funzione dummy per inizializzare i processi worker."""
    pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Setup Processi
    ctx = multiprocessing.get_context("spawn")
    log_queue = ctx.Queue()

    # 2. INIT API (Rimuovi db_manager.init_db() da qui!)
    global dmm_api
    dmm_api = DMMApi()

    # 3. INIT EXECUTOR
    global executor
    executor = ProcessPoolExecutor(
        max_workers=MAX_WORKERS,
        mp_context=ctx, 
        initializer=_init_worker,
        initargs=(log_queue,)
    )

    # Warmup
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(executor, _executor_warmup)
    
    # 4. AVVIO LOGGING
    global log_listener
    log_listener = configure_main_logging(log_queue)
    log_listener.start()
    
    logger.info("🛰️  Orbital avviato (Processi Worker isolati e pronti).")
    
    # 5. INIT DB (CONNESSIONE + CREAZIONE TABELLE)
    # Questa funzione ora fa tutto: crea il pool E lancia init_db()
    await db_manager.connect() 

    # 6. Avvio DB Writer Task
    global db_queue, db_task
    db_queue = asyncio.Queue()
    db_task = asyncio.create_task(db_writer_worker(db_queue))
    
    yield
    
    # --- SHUTDOWN ---
    logger.info("🛑 Chiusura servizi...")
    
    if dmm_api: await dmm_api.close()
    
    if db_task:
        await db_queue.put(None)
        await db_task
    
    await db_manager.close()
        
    if executor:
        executor.shutdown(wait=True)
        
    if log_listener:
        log_listener.stop()
        
    logger.info("👋 Shutdown completato.")

app = FastAPI(
    title="Orbital",
    description="Indexer Prowlarr ad alte prestazioni per Debrid Media Manager",
    version="1.0.0",  # Qui metti la tua versione (es. v2.0-Alpha)
    lifespan=lifespan,
    # Disabilitiamo la docs automatica per poterla personalizzare (vedi punto 2)
    docs_url=None, 
    redoc_url=None
)

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Documentation",
        # Qui puoi mettere un URL pubblico (es. un logo su imgur o github)
        # Oppure un'emoji convertita in base64 se vuoi fare presto
        swagger_favicon_url="https://img.icons8.com/color/48/rocket.png" 
    )

@app.get("/redoc", include_in_schema=False)
async def redoc_html():
    from fastapi.openapi.docs import get_redoc_html
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - ReDoc",
        with_google_fonts=True,
        swagger_favicon_url="https://img.icons8.com/color/48/rocket.png"
    )

# ==========================================
# CORE LOGIC (Orchestrator)
# ==========================================

async def search_orchestrator(
    metadata_task: asyncio.Task, 
    imdb_id: str,
    media_type: str,
    req_profile, 
    all_profiles, 
    user_queue: asyncio.Queue, 
    global_limits,
    req_season,
    req_episode
):
    raw_batch_queue = asyncio.Queue()
    producer_task = None
    loop = asyncio.get_running_loop()

    try:
        logger.info(f"🏁 [Orchestrator] Avvio job per {imdb_id}")
        
        # Gestione Timestamp (con try/except per non bloccare)
        try:
            if media_type == 'series' and req_season is not None:
                await db_manager.update_series_last_searched(imdb_id, req_season)
            else:
                await db_manager.update_last_searched(imdb_id)
        except Exception as e:
            logger.warning(f"⚠️ Timestamp update failed: {e}")
        
        # Producer
        async def dmm_producer():
            try:
                async for batch in dmm_api.search_with_availability(imdb_id, media_type, req_season, MAX_PAGES):
                    if batch: await raw_batch_queue.put(batch)
            except Exception as e:
                logger.error(f"❌ [DMM Producer] Error: {e}")
            finally:
                await raw_batch_queue.put(None)

        producer_task = asyncio.create_task(dmm_producer())

        # Metadata
        ctx = None
        try:
            ctx = await metadata_task
        except Exception as e:
            logger.error(f"❌ Metadata fail: {e}")
            return

        # Consumer Loop
        while True:
            raw_batch = await raw_batch_queue.get()
            if raw_batch is None: break
            
            # 1. Deduplica
            unique_batch = []
            seen_hashes = set()
            batch_map = {} 
            for item in raw_batch:
                h = item.get('hash')
                if h and h not in seen_hashes:
                    seen_hashes.add(h)
                    unique_batch.append(item)
                    batch_map[h] = item.get('title', 'Unknown')
            
            if not unique_batch: continue
            
            # LOG DEBUG
            logger.info(f"🔍 [Orchestrator] Batch grezzo: {len(unique_batch)} items. Invio al worker...")

            # 2. Worker Processing
            processed_batch = processing.cpu_process_batch(
                    unique_batch,
                    batch_map, 
                    ctx, 
                    all_profiles,
                    global_limits,
                    req_profile.id,
                    req_season,
                    req_episode
                )

            # LOG DEBUG
            logger.info(f"🔙 [Orchestrator] Worker ha restituito: {len(processed_batch)} items validi.")

            if not processed_batch:
                logger.warning("⚠️ Il worker ha scartato TUTTI gli elementi del batch! (Check score threshold?)")
                continue

            # 3. Fast Lane
            fast_lane_items = [i['_fast_lane_payload'] for i in processed_batch if i.get('_fast_lane_ready')]
            if fast_lane_items:
                logger.info(f"🚀 [Fast Lane] Invio {len(fast_lane_items)} item.")
                for fi in fast_lane_items: user_queue.put_nowait(fi)

            # 4. DB Write Preparation
            clean_batch = []
            for item in processed_batch:
                row = item.copy()
                # Rimuoviamo chiavi interne
                for k in ['_fast_lane_ready', '_fast_lane_payload', '_score', '_scores']:
                    row.pop(k, None)
                
                # Appiattimento Serie (se necessario)
                if item['media_type'] == 'series' and 'seasons_data' in item:
                    for s_data in item['seasons_data']:
                        s_row = row.copy()
                        del s_row['seasons_data']
                        s_row.update(s_data)
                        s_row['file_ids'] = str(s_data['file_ids']) # Importante: stringify
                        clean_batch.append(s_row)
                else:
                    # Movie
                    if 'file_id' in row: row['file_ids'] = str(row.pop('file_id'))
                    row['season'] = None
                    row['episodes'] = None
                    row['complete'] = False
                    clean_batch.append(row)

            # LOG DEBUG
            logger.info(f"💾 [Orchestrator] Sto per scrivere {len(clean_batch)} righe nel DB...")

            # 5. DB Write
            if clean_batch:
                try:
                    db_queue.put_nowait(clean_batch)
                except Exception as e:
                    logger.error(f"❌ Errore accodamento DB: {e}")

    except Exception as e:
        logger.error(f"☠️ [Orchestrator] Crash: {e}", exc_info=True)
    finally:
        if producer_task and not producer_task.done():
            producer_task.cancel()
            try: await producer_task
            except: pass
        user_queue.put_nowait(None)


# ==========================================
# UTILS
# ==========================================

def parse_torznab_params(request: Request) -> TorznabParams:
    params = request.query_params
    if params.get("t") == "caps": return TorznabParams(True, None, None, None, None, None, 0, 0)
    imdb_id = params.get("imdbid")
    if not imdb_id:
        if params.get('q'): return TorznabParams(False, None, True, None, None, None, 0, 0)
        return TorznabParams(False, None, None, None, None, None, 0, 0)
    if not str(imdb_id).startswith("tt"): imdb_id = f"tt{imdb_id}"
    media_type = "movie" if params.get("t") == "movie" else "series"
    season = int(params.get("season")) if params.get("season") else None
    episode = int(params.get("ep")) if params.get("ep") else None
    return TorznabParams(False, imdb_id, None, media_type, season, episode, int(params.get("limit", 100)), int(params.get("offset", 0)))

async def ensure_metadata_state(imdb_id, media_type, season):
    titles_map, year, blueprints = await MetadataService.get_standard_metadata(imdb_id, media_type)
    if media_type == 'series' and TVDB_API_KEY:
        do_refresh = not blueprints or (season and season not in blueprints)
        if not do_refresh:
            now = datetime.now()
            for bp in blueprints.values():
                if bp.next_air_date and now >= bp.next_air_date:
                    do_refresh = True; break
        if do_refresh:
            try: blueprints = await MetadataService.refresh_tvdb_blueprints(imdb_id, active_season=season)
            except: pass
    
    canonical = titles_map.get('English', 'Unknown')
    return JobContext(imdb_id, media_type, titles_map, year, canonical, blueprints)


# ==========================================
# ENDPOINTS
# ==========================================

@app.get("/", include_in_schema=False)
async def root():
    """Reindirizza la root alla documentazione Swagger UI."""
    return RedirectResponse(url="/docs")

@app.post("/api/profiles")
async def create_profile(profile: UserProfileCreate):
    """Crea un nuovo profilo utente."""
    # Convertiamo in dict
    data = profile.dict()
    
    # Eseguiamo la query (è veloce, ma usiamo to_thread per coerenza)
    new_id = await db_manager.create_user(data)
    
    if new_id == -1:
        return Response(status_code=409, content=f"Errore: L'utente '{profile.username}' esiste già.")
    
    logger.info(f"👤 [USER] Creato nuovo profilo: {profile.username} (ID: {new_id})")
    return {"status": "created", "id": new_id, "username": profile.username}

@app.get("/api/config/{user_id}")
async def get_user_config(user_id: int):
    """Ottiene la configurazione attuale di un profilo utente."""
    profile = db_manager.get_profile_by_id(user_id)
    if not profile: return Response(status_code=404, content="Utente non trovato")
    return profile

@app.patch("/api/config/{user_id}")
async def update_user_config(user_id: int, config: UserProfileUpdate):
    """Aggiorna le impostazioni di un profilo utente."""
    profile = db_manager.get_profile_by_id(user_id)
    if not profile: return Response(status_code=404, content="Utente non trovato")
    
    # Escludiamo i campi None dal payload
    updates = config.dict(exclude_unset=True)
    db_manager.update_profile(user_id, updates)
    return {"status": "ok", "updated": updates}

@app.delete("/api/torrents/{torrent_id}")
async def delete_torrent(torrent_id: int):
    """
    Elimina un torrent specifico dal DB.
    Da chiamare quando Sonarr/Radarr fallisce il download (Blackhole).
    """
    db_manager.delete_torrent_by_id(torrent_id)
    return {"status": "deleted", "id": torrent_id}

@app.get("/api/torrents/{imdb_id}")
async def get_torrents_list(imdb_id: str):
    """
    Debug: Restituisce tutti i torrent presenti nel DB per un IMDB ID.
    Utile per verificare cosa c'è in cache.
    """
    items = db_manager.get_torrents_by_imdb(imdb_id)
    return {"count": len(items), "items": items}

@app.post("/api/cache/invalidate/{imdb_id}")
async def invalidate_cache(imdb_id: str, season: Optional[int] = None):
    """
    Forza una nuova ricerca su DMM per questo specifico ID.
    - Se passi ?season=X resetta solo quella stagione.
    - Se non passi nulla, resetta tutto lo show/film.
    """
    if not imdb_id.startswith("tt"): 
        imdb_id = f"tt{imdb_id}"
        
    # Eseguiamo nel thread pool per non bloccare
    await asyncio.to_thread(db_manager.reset_last_searched, imdb_id, season)
    
    msg_suffix = f" (Stagione {season})" if season is not None else " (Global)"
    logger.info(f"🔄 [CACHE] Invalidata cache per {imdb_id}{msg_suffix}.")
    
    return {
        "status": "ok", 
        "message": f"Cache invalidata per {imdb_id}", 
        "scope": "season" if season else "global"
    }

@app.get("/api/{username}")
async def torznab_endpoint(username: str, request: Request):
    req = parse_torznab_params(request)
    if req.is_caps: return Response(content=CAPS_XML.strip(), media_type="application/xml")
    if not req.imdb_id:
        if req.q: return Response(content=SearchService.generate_xml([], username), media_type="application/xml")
        return Response(content=SearchService.generate_dummy_xml(username), media_type="application/xml")

    # 1. Load Profile
    user_ctx = await db_manager.get_profiles_infos(username)
    current_profile, all_profiles, global_limits = user_ctx
    if not current_profile: return Response(status_code=401)

    # 2. Check Cache
    cached_candidates = await db_manager.get_cached_scored(
        req.imdb_id, 
        current_profile, 
        req.season, 
        req.episode
    )
    
    # Contiamo quanti risultati "Eccellenti" abbiamo già in cache
    # (Lo score è già presente nella chiave '_score' calcolata dal DB)
    exc_count = sum(1 for x in cached_candidates if x['_score'] >= current_profile.preferred_score_threshold)

    # 3. Fast Return (Cache Hit)
    if exc_count >= FAST_LANE_THRESHOLD:
        logger.info(f"⚡ [CACHE HIT] {exc_count} eccellenti pronti da DB.")
        return Response(content=SearchService.generate_xml(cached_candidates[:RESULTS_NUM], username), media_type="application/xml")

    if req.season:
        meta = await db_manager.get_season_metadata(req.imdb_id, req.season)
    else:
        meta = await db_manager.get_metadata(req.imdb_id)
        
    should_search = True
    
    if meta and meta.get('last_searched'):
        try:
            # Fix timestamp string
            last_search_str = str(meta['last_searched']).split('.')[0]
            last_search = datetime.strptime(last_search_str, "%Y-%m-%d %H:%M:%S")
            
            # Calcolo tempo trascorso
            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
            elapsed = (now_utc - last_search).total_seconds()
            
            if elapsed < SEARCH_CACHE_TTL:
                logger.info(f"⏳ [TTL SKIP] Ricerca recente ({int(elapsed)}s fa).")
                should_search = False
            
        except Exception as e:
            logger.warning(f"Errore parsing data cache: {e}")
            should_search = True

    if should_search:

        # 5. Live Search (Cache Miss)
        logger.info(f"🌐 [LIVE SEARCH] Avvio scraping parallelo per {req.imdb_id}...")
        
        # --- PARALLELIZZAZIONE ---
        # 1. Creiamo il task per i metadati (TMDB/TVDB) ma NON lo attendiamo qui
        meta_task = asyncio.create_task(ensure_metadata_state(req.imdb_id, req.media_type, req.season))
        
        user_queue = asyncio.Queue()
        
        # 2. Passiamo il task all'orchestrator, insieme ai parametri raw per partire subito con DMM
        asyncio.create_task(search_orchestrator(
            meta_task,
            req.imdb_id,
            req.media_type,
            current_profile, 
            all_profiles, 
            user_queue, 
            global_limits, 
            req.season, 
            req.episode
        ))
        
        collected_live = []
        live_exc_count = 0
        start_time = time.time()
        
        # Mappa deduplicazione live (InfoHash -> Item) per evitare doppioni tra cache e live
        seen_hashes = {f"{i['info_hash']}_{i['file_ids']}" for i in cached_candidates}

        try:
            while True:
                # Timeout totale di sicurezza per la risposta HTTP (es. 45s)
                if time.time() - start_time > 45: break
                
                try:
                    # Aspettiamo il prossimo item dalla Fast Lane
                    item = await asyncio.wait_for(user_queue.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    # Se non arriva nulla per 2 secondi, controlliamo se abbiamo abbastanza roba
                    if live_exc_count > 0: break 
                    continue

                if item is None: # Segnale di fine stream
                    break
                
                # Deduplica
                uid = f"{item['info_hash']}_{item['file_ids']}"
                if uid in seen_hashes: continue
                seen_hashes.add(uid)
                
                collected_live.append(item)
                if item['_score'] >= current_profile.preferred_score_threshold:
                    live_exc_count += 1
                
                # Se abbiamo raggiunto la soglia combinata (Cache + Live), usciamo felici
                if (exc_count + live_exc_count) >= FAST_LANE_THRESHOLD:
                    logger.info(f"🚀 [FAST LANE] Raggiunta soglia ({exc_count + live_exc_count}). Invio risposta.")
                    break

        except Exception as e:
            logger.error(f"Errore loop attesa: {e}")

        # 6. Merge e Risposta Finale
        final_list = cached_candidates + collected_live
    
    else:
        # Se NON cerchiamo, ritorniamo solo quello che avevamo in cache (anche se poco/nullo)
        final_list = cached_candidates
    
    # 6. Risposta Finale
    final_list.sort(key=lambda x: x['_score'], reverse=True)
    
    return Response(content=SearchService.generate_xml(final_list[:RESULTS_NUM], username), media_type="application/xml")

if __name__ == "__main__":
    # Avvio Uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8282)