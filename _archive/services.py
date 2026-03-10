import httpx
import asyncio
import time
import email.utils
import html
import pycountry
import tvdb_v4_official
from collections import defaultdict
from datetime import datetime
from config import TMDB_API_KEY, TVDB_API_KEY, get_logger
from database import db_manager
from models import SeriesBlueprint, UserProfile
from utils import res_rank

logger = get_logger("Services")

class MetadataService:
    @staticmethod
    async def get_standard_metadata(imdb_id: str, media_type: str):
        """
        Recupera metadati (Titoli, Anno, Blueprints).
        Usa la cache DB (media_metadata) se disponibile, altrimenti chiama TMDB.
        """
        titles_map = {}
        year = None
        blueprints = {}
        
        # 1. CHECK CACHE METADATI (Titoli e Anno)
        # Se li abbiamo già nel DB, evitiamo la chiamata a TMDB
        cached_meta = await db_manager.get_metadata(imdb_id)
        
        if cached_meta:
            # Cache Hit! Usiamo i dati locali
            try:
                titles_map = cached_meta.get('titles', {})
                year = cached_meta['year']
                # logger.info(f"💾 [CACHE] Metadati statici trovati per {imdb_id}")
            except Exception as e:
                logger.error(f"Errore parsing JSON metadati: {e}")
                cached_meta = None # Forziamo refresh se il JSON è corrotto
        
        # 2. CACHE MISS? -> CHIAMATA TMDB
        if not cached_meta:
            # logger.info(f"🌍 [API] Scarico metadati da TMDB per {imdb_id}")
            endpoint = "tv" if media_type == "series" else "movie"
            tmdb_id = None
            
            async with httpx.AsyncClient() as client:
                # A. Find by IMDB ID
                try:
                    r = await client.get(
                        f"https://api.themoviedb.org/3/find/{imdb_id}",
                        params={"api_key": TMDB_API_KEY, "external_source": "imdb_id"}
                    )
                    if r.status_code == 200:
                        data = r.json()
                        results = data.get(f'{endpoint}_results', [])
                        if results:
                            tmdb_item = results[0]
                            tmdb_id = tmdb_item.get('id')
                            # Tentativo recupero anno immediato dal risultato find
                            date_field = 'first_air_date' if media_type == 'series' else 'release_date'
                            date_str = tmdb_item.get(date_field)
                            if date_str: year = int(date_str[:4])
                except Exception as e:
                    logger.error(f"TMDB Find Error: {e}")

                if tmdb_id:
                    # B. Fetch details + Translations
                    target_languages = ['en-US', 'it-IT']
                    tasks = []
                    url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}"
                    
                    for lang in target_languages:
                        tasks.append(client.get(url, params={"api_key": TMDB_API_KEY, "language": lang}))
                    
                    responses = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    en_data = None
                    for i, resp in enumerate(responses):
                        if isinstance(resp, Exception) or resp.status_code != 200: continue
                        data = resp.json()
                        lang_code = target_languages[i]
                        
                        if lang_code == 'en-US': en_data = data
                        
                        # Mapping Lingua
                        iso_lang = pycountry.languages.get(alpha_2=lang_code[:2]).name
                        t_val = data.get('title' if media_type == 'movie' else 'name')
                        if t_val: titles_map[iso_lang] = t_val

                    # Titolo originale
                    if en_data:
                        orig = en_data.get('original_title' if media_type == 'movie' else 'original_name')
                        orig_lang_code = en_data.get('original_language')
                        if orig and orig_lang_code:
                            try:
                                l_obj = pycountry.languages.get(alpha_2=orig_lang_code)
                                if l_obj: titles_map[l_obj.name] = orig
                            except: pass
                        
                        # Anno (se non trovato prima)
                        if not year:
                            date_field = 'first_air_date' if media_type == 'series' else 'release_date'
                            date_str = en_data.get(date_field)
                            if date_str: year = int(date_str[:4])

            # 3. SALVATAGGIO CACHE (Per il futuro)
            if titles_map:
                await db_manager.save_metadata({
                    'imdb_id': imdb_id,
                    'tmdb_id': tmdb_id,
                    'media_type': media_type,
                    'year': year,
                    'titles': titles_map
                })

        # 4. BLUEPRINTS (Solo Series) - Logica invariata, legge sempre DB locale
        if media_type == "series":
            blueprints = await db_manager.get_series_blueprints(imdb_id)

        return titles_map, year, blueprints

    @staticmethod
    async def refresh_tvdb_blueprints(imdb_id: str, active_season: int = None) -> dict:
        """Wrapper async: Scarica nel thread, Salva nel main loop."""
        # 1. Scarica i dati nel thread (Sincrono)
        # Nota: Ho rinominato il metodo interno in _fetch_tvdb_blueprint_sync per chiarezza
        result = await asyncio.to_thread(MetadataService._fetch_tvdb_blueprint, imdb_id)
        
        if result:
            series_name, blueprints = result
            # 2. Scrivi nel DB (Asincrono) - Questo risolve l'errore di connessione
            await db_manager.put_series_blueprints(series_name, imdb_id, blueprints, active_season)
            return blueprints
        return {}

    @staticmethod
    def _fetch_tvdb_blueprint(imdb_id: str):
        """
        Logica Sincrona pura per TVDB. Restituisce (series_name, blueprints) o None.
        """
        # Assicurati di avere questo import in alto nel file services.py:
        # from datetime import datetime 
        
        blueprints = {}
        series_name = None # Inizializziamo a None per sicurezza

        try:
            tvdb = tvdb_v4_official.TVDB(TVDB_API_KEY)
            
            # 1. Cerca la serie tramite REMOTE ID (IMDB)
            res = tvdb.search_by_remote_id(imdb_id)
            if not res:
                logger.warning(f"TVDB: Nessuna serie trovata per {imdb_id}")
                return None # RETURN CONSISTENTE
            
            # 2. Trova l'ID corretto della serie
            series_id = None
            for item in res:
                series_obj = item.get('series')
                if series_obj:
                    series_id = series_obj.get('id')
                    series_name = series_obj.get('slug')
                    if series_id: break
                elif item.get('type') == 'series':
                    series_id = item.get('tvdb_id') or item.get('id')
                    # Se il nome non l'abbiamo trovato nel blocco 'series', proviamo qui
                    if not series_name: series_name = item.get('name')
                    if series_id: break
            
            if not series_id and res:
                 series_obj = res[0].get('series')
                 if series_obj: 
                     series_id = series_obj.get('id')
                     if not series_name: series_name = series_obj.get('name')

            if not series_id: 
                logger.warning(f"TVDB: ID non trovato nel payload search per {imdb_id}")
                return None # RETURN CONSISTENTE

            # 3. Ottieni dettagli estesi della SERIE
            series_data = tvdb.get_series_extended(series_id)
            
            # Fallback finale per il nome
            if not series_name: series_name = series_data.get('name', 'Unknown Series')
            
            status_obj = series_data.get('status', {})
            series_status_name = status_obj.get('name', '').lower() 
            is_series_ended = (series_status_name == 'ended')
            
            all_seasons_meta = series_data.get('seasons', [])
            
            # 4. Filtriamo e approfondiamo le stagioni
            for s_meta in all_seasons_meta:
                s_type = s_meta.get('type', {})
                # ID 1 = Aired Order (Default)
                if s_type.get('id') != 1 and s_type.get('name') != 'Aired Order':
                    continue

                s_num = s_meta.get('number')
                s_id = s_meta.get('id')
                
                if s_num == 0: continue 
                if not s_id: continue

                is_season_ended = is_series_ended

                try:
                    season_details = tvdb.get_season_extended(s_id)
                    episodes_list = season_details.get('episodes', [])
                    ep_count_total = len(episodes_list)
                    if ep_count_total == 0: continue
                    next_air_date = None

                    # Setup Variabili
                    now = datetime.now()
                    latest_aired_episode = 0
                    
                    if is_series_ended:
                        latest_aired_episode = ep_count_total
                        is_season_ended = True
                    else:
                        # Iteriamo AL CONTRARIO
                        for idx, ep in enumerate(reversed(episodes_list)):
                            aired = ep.get("aired")
                            if not aired: continue 

                            try:
                                aired_date = datetime.strptime(aired, "%Y-%m-%d")
                                if aired_date <= now:
                                    latest_aired_episode = ep_count_total - idx
                                    break # Trovato l'ultimo uscito
                                # Se siamo qui, la data è futura
                                next_air_date = aired_date 
                            except ValueError:
                                continue
                        
                        if latest_aired_episode >= ep_count_total:
                            is_season_ended = True
                        else:
                            is_season_ended = False

                except Exception as e:
                    logger.warning(f"TVDB: Errore fetch stagione {s_num} (ID {s_id}): {e}")
                    continue
                
                blueprints[s_num] = SeriesBlueprint(
                    season_number=s_num,
                    episode_count=ep_count_total,
                    latest_aired_episode=latest_aired_episode,
                    is_ended=is_season_ended,
                    next_air_date=next_air_date
                )
            
            logger.info(f"✅ TVDB Blueprint per {imdb_id}: Trovate {len(blueprints)} stagioni.")
            
            # SUCCESSO: Ritorniamo la tupla
            return series_name, blueprints
            
        except Exception as e:
            logger.error(f"Errore interno TVDB Worker: {e}")
            return None # FALLIMENTO: Ritorniamo None

class SearchService:
    @staticmethod
    def calculate_score(item: dict, profile: UserProfile) -> int:
        i_lang = str(item['languages'] or "")
        i_res_str = str(item['resolution'] or "")
        i_audio = str(item['audio'] or "")
        i_hdr = str(item['hdr'] or "")
        i_res_rank = res_rank(i_res_str)

        # 1. Filtri Hard
        if profile.max_size > 0 and item['file_size'] > profile.max_size: return -1000
        if i_res_rank > profile.max_resolution: return -1000
        
        score = 0
        
        # 2. Punteggi Tecnici (Profilo Utente)
        if "Italian" in i_lang: score += profile.score_ita
        if item['remux']: score += profile.score_remux
        if "Dolby Vision" in i_hdr: score += profile.score_dv
        if "Atmos" in i_audio: score += profile.score_atmos
        if "HDR" in i_hdr and "Dolby Vision" not in i_hdr: score += profile.score_hdr
        
        if "2160" in i_res_str: score += profile.score_2160p
        elif "1080" in i_res_str: score += profile.score_1080p
        elif "720" in i_res_str: score += profile.score_720p
        
        # 3. Punteggi Struttura (Episodi / Stagioni)
        if item['media_type'] == 'series':
            # Calcolo numero episodi
            ep_count = 0
            eps = item.get('episodes')
            
            if isinstance(eps, (list, set, tuple)):
                ep_count = len(eps)
            elif isinstance(eps, str) and eps:
                import re
                # Conta quanti numeri ci sono nella stringa es: "{1, 2, 3}" -> 3
                ep_count = len(re.findall(r'\d+', eps))
            
            # VALORE X = 10 punti per episodio
            score += (ep_count * 10)
            
            # Bonus Stagione Completa
            if item.get('complete') == 1:
                score += 50

        return score

    @staticmethod
    def select_top_items(all_items: list[dict], profiles: list[UserProfile]) -> list[dict]:
        """
        Seleziona i migliori risultati per ogni profilo.
        OTTIMIZZAZIONE STEP 4: Rimosso il calcolo score nel main thread.
        Si aspetta che '_scores' sia già stato calcolato dai worker o dalla cache logic.
        """
        keepers = {} # Usa un dict {unique_id: item} per deduplicare automaticamente
        
        # Funzione helper per generare ID univoco
        def get_uid(i): return f"{i['info_hash']}_{i['file_ids']}"

        for prof in profiles:
            valid_movies = []
            packs = defaultdict(list)
            singles_map = defaultdict(list)
            
            for item in all_items:
                # --- MODIFICA QUI ---
                # Non calcoliamo più lo score se manca. Se manca, l'item è invalido/corrotto.
                score = item.get('_score')
                
                # Se per qualche motivo non c'è lo score per questo utente (caso raro ma possibile)
                if score is None:
                    continue

                # Filtro soglia minima (Hard Filter)
                if score < prof.min_score_threshold:
                    continue

                # Creiamo una copia leggera per uso interno
                entry = item # Non facciamo .copy() qui se non serve modificarlo, risparmiamo CPU
                # Salviamo lo score temporaneo per l'ordinamento
                entry['_temp_score'] = score

                if entry['media_type'] == 'movie':
                    valid_movies.append(entry)
                    continue
                
                if entry.get('complete'): 
                    packs[entry['season']].append(entry)
                else:
                    # Gestione episodi (lista o set)
                    eps = entry.get('episodes')
                    if eps:
                        # Se è un set/list, iteriamo
                        if isinstance(eps, (list, set, tuple)):
                            for ep in eps:
                                singles_map[ep].append(entry)
                        # Se è int (caso raro legacy)
                        elif isinstance(eps, int):
                            singles_map[eps].append(entry)
            
            # --- SELEZIONE TOP ITEMS (Logica invariata) ---
            if valid_movies:
                valid_movies.sort(key=lambda x: x['_temp_score'], reverse=True)
                for m in valid_movies[:6]:
                    keepers[get_uid(m)] = m
            else:
                # A. Top 3 Packs
                for season, items in packs.items():
                    items.sort(key=lambda x: x['_temp_score'], reverse=True)
                    for p in items[:6]:
                        keepers[get_uid(p)] = p

                # B. Top 2 Singles per Episodio
                for ep, candidates in singles_map.items():
                    candidates.sort(key=lambda x: x['_temp_score'], reverse=True)
                    for s in candidates[:2]:
                        keepers[get_uid(s)] = s

        # Pulizia finale: Rimuoviamo il campo temporaneo _temp_score
        final_list = []
        for item in keepers.values():
            # Rimuoviamo _temp_score se presente (sicurezza)
            try:
                del item['_temp_score']
            except KeyError:
                pass
            final_list.append(item)
            
        return final_list
    
    @staticmethod
    def generate_dummy_xml(username: str) -> str:
        """
        Genera XML di test. 
        """
        dummy_date = "Wed, 01 Jan 2020 00:00:00 -0000"
        # FIX QUI: & -> &amp;
        magnet = "magnet:?xt=urn:btih:0000000000000000000000000000000000000000&amp;dn=Orbital.Test&amp;indices=0"
        
        return f"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0" xmlns:torznab="http://torznab.com/schemas/2015/feed">
            <channel>
                <title>Orbital - {username}</title>
                <link>http://orbital:8282/</link>
                <item>
                    <title>Orbital Connection Test - Success</title>
                    <guid isPermaLink="false">orbital_test_01</guid>
                    <link>{magnet}</link>
                    <size>1048576</size>
                    <pubDate>{dummy_date}</pubDate>
                    <enclosure url="{magnet}" length="1048576" type="application/x-bittorrent" />
                    <category>2000</category>
                    <category>5000</category>
                </item>
            </channel>
        </rss>"""
    
    # --- INIZIO MODIFICA services.py ---

    @staticmethod
    def generate_xml(items: list[dict], username: str) -> str:
        # OTTIMIZZAZIONE XML: Uso lista e join invece di concatenazione stringhe
        xml_parts = []
        
        # Header statico
        xml_parts.append(f"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0" xmlns:torznab="http://torznab.com/schemas/2015/feed">
            <channel>
                <title>Orbital - {username}</title>
                <link>http://orbital:8282/</link>""")
        
        for item in items:
            # 1. Preparazione Magnet
            # Nota: F-string è più veloce di .format() o concatenazione
            raw_magnet = f"magnet:?xt=urn:btih:{item['info_hash']}&dn={item['title']}&indices={item['file_ids']}"
            magnet = raw_magnet.replace("&", "&amp;")
            
            # 2. Escaping Titolo
            title_to_use = item.get('arr_title') or item['title']
            safe_title = html.escape(title_to_use)
            
            # 3. Attributi Torznab (Costruiamo la stringa attributi)
            attr_list = []
            attr_list.append(f'<torznab:attr name="imdbid" value="{item["imdb_id"].replace("tt","")}" />')
            
            # Gestione specifica Serie TV
            if item['media_type'] == 'series':
                if item.get('season') is not None:
                    attr_list.append(f'<torznab:attr name="season" value="{item["season"]}" />')
                
                # Logica Episodi
                eps = item.get('episodes')
                is_complete = item.get('complete', 0)
                
                if not is_complete and eps:
                    if isinstance(eps, str):
                        # Caso stringa DB
                        import re
                        nums = re.findall(r'\d+', eps)
                        if len(nums) == 1:
                            attr_list.append(f'<torznab:attr name="episode" value="{nums[0]}" />')
                    elif isinstance(eps, (list, set, tuple)):
                        # Caso lista/set Fast Lane
                        if len(eps) == 1:
                            attr_list.append(f'<torznab:attr name="episode" value="{list(eps)[0]}" />')

            # Seeders/Peers finti per compatibilità
            attr_list.append('<torznab:attr name="seeders" value="100" />')
            attr_list.append('<torznab:attr name="peers" value="100" />')
            
            attrs_str = "".join(attr_list)

            # Aggiunta Item alla lista principale
            xml_parts.append(f"""
            <item>
                <title>{safe_title}</title>
                <guid isPermaLink="false">{item['info_hash']}_{item['file_ids']}</guid>
                <link>{magnet}</link>
                <size>{item['file_size']}</size>
                <pubDate>{email.utils.formatdate(time.time())}</pubDate>
                <enclosure url="{magnet}" length="{item['file_size']}" type="application/x-bittorrent" />
                <category>{"2000" if item['media_type']=="movie" else "5000"}</category>
                {attrs_str}
            </item>""")
            
        # Chiusura XML
        xml_parts.append("""
            </channel>
        </rss>""")
        
        return "".join(xml_parts)