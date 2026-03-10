import re
import bisect
from pathlib import Path
from rapidfuzz import fuzz
import PTN
from functools import lru_cache
from config import get_logger

logger = get_logger("Validation")

# ==========================================
# REGEX & COSTANTI
# ==========================================

SKIP_KEYWORDS = {'sample', 'trailer', 'feature', 'extra', 'bonus', 'interview', 'documentary', 'behind the scenes'}
video_exts = ('.mkv', '.mp4', '.avi')

# Regex 1: Standard S01E01, S01.E01, S01_E01
RE_S_E = re.compile(r'(?i)(?:^|[ ._-])S(?P<s>\d{1,2})(?:[ ._-]*E(?P<e>\d{1,3}))\b')

# Regex 2: Strict NxM (1x01) - Invariata (richiede spazio o nulla, no punti)
RE_N_X_M = re.compile(r'(?i)\b(?P<s>\d{1,2})\s*x\s*(?P<e>\d{1,3})\b')

# Regex 3: Bracket Sxx [xx] (Anime) - FIXATO
RE_S_BRACKET = re.compile(r'(?i)(?:^|[ ._-])S(?P<s>\d{1,2})(?:[ ._-]*)\[(?P<e>\d{1,3})\]')

# Regex 4: Solo Episodio
RE_EP_ONLY = re.compile(r'(?i)(?:\b(?:E|Ep|Episode|Episodio|Puntata)[ ._-]*(?P<e>\d{1,3})\b|^(?P<e_start>\d{1,3})[ ._-])')

# Regex 5: Solo Stagione
RE_SEASON_ONLY = re.compile(r'(?i)(?:^|[ ._-])(?:S|Season|Stagione)[ ._-]*(?P<s>\d{1,2})\b')

# ==========================================
# FUNZIONI DI PARSING
# ==========================================

def get_data_from_file(file_entry, media_type):
    raw_files = file_entry.get('files', [])
    if not raw_files: return None
    
    # --- FASE 1: Censimento File Validi ---
    valid_files = []
    
    for f in raw_files:
        # OTTIMIZZAZIONE PATH: Manipolazione stringa invece di oggetto Path
        path_str = f['path'].lstrip("/")
        
        # Divisione in parti (molto più veloce di Path.parts)
        parts = path_str.split('/')
        filename = parts[-1]
        
        # Filtro estensione (case insensitive check manuale o endswith tuple)
        if not filename.lower().endswith(video_exts): continue
        
        # Filtro Junk words
        # path_str include tutto, quindi il check è valido
        path_lower = path_str.lower()
        if any(k in path_lower for k in SKIP_KEYWORDS): continue
        
        # Salviamo parts invece di path_obj per usarle dopo
        valid_files.append((parts, filename, f['file_id'], f['bytes']))

    if not valid_files: return None

    # --- LOGICA MOVIE ---
    if media_type == 'movie':
        candidates = {fid: size for _, _, fid, size in valid_files}
        if not candidates: return None
        max_size = max(candidates.values())
        return [fid for fid, size in candidates.items() if size >= max_size * 0.6]

    # --- LOGICA SERIES ---
    result = {} 

    for parts, filename, fid, _ in valid_files:
        # filename è già estratto sopra
        
        s_final = None
        e_final = None

        # TENTATIVO A: Info Complete nel File
        m_se = RE_S_E.search(filename)
        if m_se:
            s_final = int(m_se.group('s'))
            e_final = int(m_se.group('e'))
        
        if s_final is None:
            m_br = RE_S_BRACKET.search(filename)
            if m_br:
                s_final = int(m_br.group('s'))
                e_final = int(m_br.group('e'))

        if s_final is None:
            m_nxm = RE_N_X_M.search(filename)
            if m_nxm:
                s_final = int(m_nxm.group('s'))
                e_final = int(m_nxm.group('e'))

        # TENTATIVO B: Solo Episodio nel File + Stagione nel Padre
        if s_final is None:
            m_ep = RE_EP_ONLY.search(filename)
            if m_ep:
                e_final = int(m_ep.group('e') or m_ep.group('e_start'))
                
                # Cerchiamo la stagione nei padri (fino a 2 livelli su)
                # parts[-1] è il file. parts[-2] è il genitore immediato.
                
                parents_to_check = []
                if len(parts) > 1: parents_to_check.append(parts[-2])
                if len(parts) > 2: parents_to_check.append(parts[-3])

                for folder_name in parents_to_check:
                    m_sea = RE_SEASON_ONLY.search(folder_name)
                    if m_sea:
                        s_final = int(m_sea.group('s'))
                        break
        
        # --- Assegnazione Risultati ---
        if e_final is not None:
            if s_final is not None:
                if s_final not in result: result[s_final] = {'eps': {}, 'ids': []}
                result[s_final]['eps'][e_final] = fid
                result[s_final]['ids'].append(fid)
            else:
                if 'unknown' not in result: result['unknown'] = {}
                result['unknown'][e_final] = fid

    return result


def resolve_unknown_episodes(data, blueprints):
    # Se non c'è nulla di sconosciuto o non abbiamo la mappa blueprint, esci subito
    if 'unknown' not in data or not data['unknown'] or not blueprints:
        return data

    unknown_items = sorted(data.pop('unknown').items(), key=lambda x: x[0])
    
    soglie = []
    stagioni_ids = []
    accumulatore = 0
    
    sorted_keys = sorted([int(k) for k in blueprints.keys()])
    


    for s_id in sorted_keys:
        try:
            bp = blueprints[s_id]
        except KeyError: bp = bp = blueprints[str(s_id)]
        ep_count = bp.episode_count if hasattr(bp, 'episode_count') else bp['episode_count']
        accumulatore += ep_count
        soglie.append(accumulatore)
        stagioni_ids.append(s_id)

    for ep_assoluto, file_id in unknown_items:
        # Trova a quale stagione appartiene questo numero assoluto
        idx = bisect.bisect_left(soglie, ep_assoluto)
        
        if idx < len(stagioni_ids):
            target_s = stagioni_ids[idx]
            
            # --- CALCOLO NUMERO RELATIVO ---
            # Se siamo nella stagione 2 o successiva, dobbiamo sottrarre gli episodi precedenti
            # Esempio: Ep assoluto 26 (S2E02). Soglia precedente S1=24. Relativo = 26-24 = 2.
            threshold_precedente = soglie[idx-1] if idx > 0 else 0
            ep_relativo = ep_assoluto - threshold_precedente
            
            # Se il numero diventa 0 o negativo (casi limite/errori), usiamo l'assoluto o saltiamo
            if ep_relativo <= 0: ep_relativo = ep_assoluto

            # --- INIZIALIZZAZIONE STRUTTURA ---
            # Se la stagione non esiste, la creiamo
            if target_s not in data: 
                data[target_s] = {'eps': {}, 'ids': []}
            
            # --- SAFETY CHECK (Il più importante) ---
            # Se la stagione esiste già e ha già questo episodio, IGNORA l'unknown.
            # I dati già classificati (dalla regex SxxExx) sono più affidabili.
            if ep_relativo in data[target_s]['eps']:
                continue 

            # Se siamo qui, il posto è libero. Assegniamo.
            data[target_s]['eps'][ep_relativo] = file_id
            data[target_s]['ids'].append(file_id) # Aggiorniamo anche la lista flat degli ID se la usi
            
        else:
            # Overflow (numero episodio più alto di tutte le stagioni conosciute)
            pass

    return data

def _extract_media_tags(metadata, parent_title):
    """
    Helper privato: Estrae i dati tecnici (Resolution, HDR, Audio...) 
    e costruisce la stringa dei tag e il dizionario dei flag per il DB.
    """
    # 1. Estrazione dati grezzi
    resolution = metadata.get('resolution', 'Unknown')
    quality = metadata.get('quality', 'Unknown')
    codec = metadata.get('codec', 'Unknown')
    audio = metadata.get('audio', 'Unknown')
    
    # 2. Normalizzazione stringhe per check
    formatted_parent = parent_title.replace(".", " ").replace("-", " ").replace("|", " ").lower()
    
    # 3. Calcolo Flag Booleani
    is_hdr = 1 if 'hdr' in formatted_parent else 0
    is_remux = 1 if 'remux' in formatted_parent else 0
    is_dv = 1 if ('dolby vision' in formatted_parent or ' dv ' in formatted_parent or ' dovi ' in formatted_parent) else 0

    # 4. Costruzione Stringa Tags (es. " | 1080p REMUX HDR...")
    # Normalizzazione per visualizzazione
    res_str = "" if resolution == 'Unknown' else f" {resolution}"
    qual_str = "" if quality == 'Unknown' else f" {quality}"
    codec_str = "" if codec == 'Unknown' else f" {codec}"
    audio_str = "" if audio == 'Unknown' else f" {audio}"
    
    hdr_str = ' HDR' if is_hdr else ''
    remux_str = ' REMUX' if is_remux else ''
    dv_str = ' Dolby Vision' if is_dv else ''

    # Unione stringa finale
    tags_string = f"{res_str}{remux_str}{qual_str}{hdr_str}{dv_str}{audio_str}{codec_str}"

    # 5. Dizionario dati per il DB
    info_dict = {
        'resolution': resolution,
        'quality': quality,
        'codec': codec,
        'audio': audio,
        'hdr': is_hdr,
        'remux': is_remux,
        'dolby_vision': is_dv
    }
    
    return info_dict, tags_string

def build_movie_result(title, year, languages, metadata, parent_title):
    """Costruisce il risultato finale per un Film."""
    
    info_dict, tags_string = _extract_media_tags(metadata, parent_title)
    
    lang_str = "" if languages == 'Unknown' else f" | {languages}"
    year_str = f" ({year})" if year else ""
    
    if tags_string:
        arr_title = f"{title}{year_str}{lang_str} |{tags_string}"
    else:
        arr_title = f"{title}{year_str}{lang_str}"
        
    # OTTIMIZZAZIONE COPY: Modifica in-place invece di .copy()
    # info_dict è stato creato fresco da _extract_media_tags, possiamo sporcarlo
    info_dict['title'] = title
    info_dict['torrent_title'] = parent_title
    info_dict['year'] = year
    info_dict['languages'] = languages
    info_dict['arr_title'] = arr_title
    
    return info_dict

def build_series_result(title, year, languages, metadata, parent_title, seasons_data):
    """Costruisce il risultato finale per una Serie TV."""
    
    info_dict, tags_string = _extract_media_tags(metadata, parent_title)
    
    lang_str = "" if languages == 'Unknown' else f" | {languages}"
    year_str = f" ({year})" if year else ""
    
    for s in seasons_data:
        season_str = f" S{s['season']:02}"
        
        if s['complete']:
            ep_info = " Complete"
        else:
            eps = s['episodes']
            if len(eps) > 1:
                ep_info = f"E{min(eps):02}-{max(eps):02}"
            else:
                ep_info = f"E{next(iter(eps)):02}"
        
        arr_infos = f"{season_str}{ep_info}"

        if tags_string:
            s['arr_title'] = f"{title}{year_str}{arr_infos}{lang_str} |{tags_string}"
        else:
            s['arr_title'] = f"{title}{year_str}{arr_infos}{lang_str}"

    # OTTIMIZZAZIONE COPY: Modifica in-place
    info_dict['title'] = title
    info_dict['torrent_title'] = parent_title
    info_dict['year'] = year
    info_dict['languages'] = languages
    info_dict['seasons_data'] = seasons_data
    
    return info_dict

@lru_cache(maxsize=4096)
def cached_ptn_parse(title: str):
    """Wrapper cachato per PTN.parse per evitare di ri-parsare lo stesso titolo padre."""
    return PTN.parse(title)

def element_validation(element, context, parent_title):
    # Supporto Context
    media_type = context.media_type if hasattr(context, 'media_type') else context['media_type']
    titles_map = context.titles_map if hasattr(context, 'titles_map') else context['titles_map']
    tmdb_year = context.tmdb_year if hasattr(context, 'tmdb_year') else context.get('tmdb_year')
    blueprints = context.blueprints if hasattr(context, 'blueprints') else context.get('blueprints', {})

    metadata = cached_ptn_parse(parent_title)
    title_parsed = metadata.get('title')
    
    if not title_parsed: return {'status': 'FAIL', 'reason': f'Titolo illeggibile: {parent_title}'}
    # --- VALIDAZIONE TITOLO (Fuzzy Match) ---
    best_sim = 0
    prob_lang = None
    for l_code, t_map in titles_map.items():
        ratio = fuzz.ratio(title_parsed.lower(), t_map.lower())
        if ratio > best_sim: best_sim = ratio
        if ratio > 90:
            prob_lang = l_code
            break
        else:
            token_set = fuzz.token_set_ratio(title_parsed.lower(), t_map)
            if token_set < 50: continue
            if token_set > best_sim: best_sim = token_set
            if token_set > 90:
                prob_lang = l_code
                break
    
    if best_sim < 75: 
        return {'status': 'FAIL', 'reason': f'Match Titolo basso ({best_sim}%): {title_parsed}'}

    year = metadata.get('year')
    lang_final = metadata.get('language') or prob_lang or 'Unknown'
    lang_final = ", ".join(lang_final) if isinstance(lang_final, list) else str(lang_final)

    # --- LOGICA MOVIE ---
    if media_type == 'movie':
        if year and tmdb_year:
            diff = abs(int(year) - int(tmdb_year))
            if diff > 1: return {'status': 'FAIL', 'reason': f'Anno errato ({year} vs {tmdb_year})'}
            if diff == 0 and best_sim < 75: return {'status': 'FAIL', 'reason': 'Anno ok ma titolo dubbio'}

        valid_ids = get_data_from_file(element, 'movie')
        if not valid_ids: return {'status': 'FAIL', 'reason': 'Nessun file video valido'}
        if len(valid_ids) > 1: return {'status': 'FAIL', 'reason': 'Ambiguità: più file video'}

        res = build_movie_result(context.canonical_title, year, lang_final, metadata, parent_title)
        return {'status': 'MATCHED', 'info_hash': element['hash'], 'media_type': 'movie', 'file_size': sum(f['bytes'] for f in element.get('files', [])), 'file_ids': str(valid_ids[0])} | res

    # --- LOGICA SERIES ---
    elif media_type == 'series':
        data = get_data_from_file(element, 'series') # Analizza i file interni
        if not data: return {'status': 'FAIL', 'reason': 'Nessun episodio trovato'}

        if 'unknown' in data:
            data = resolve_unknown_episodes(data, blueprints)

        valid_seasons = []
        
        for s, info in data.items():
            if s == 'unknown' or s == 'overflow': continue
            
            episodes = info['eps']
            file_ids_list = info['ids']
            
            # --- NUOVA LOGICA BLUEPRINT (TVDB) ---
            # 1. Cerchiamo il blueprint
            bp = blueprints.get(s) or blueprints.get(str(s))
            if not bp: continue

            latest_aired_ep = bp.latest_aired_episode if hasattr(bp, 'latest_aired_episode') else bp['latest_aired_episode']
            for e in list(episodes): 
                if e > latest_aired_ep:
                    del episodes[e]

            if not episodes:
                # Se abbiamo cancellato tutti gli episodi perché futuri/invalidi,
                # saltiamo l'intera stagione.
                continue
            
            complete = 0
            ep_count_tvdb = 0
            
            ep_count_tvdb = bp.episode_count if hasattr(bp, 'episode_count') else bp['episode_count']
                
            # Calcolo completezza matematico (File vs TVDB)
            if len(episodes) >= ep_count_tvdb:
                complete = 1
            
            valid_seasons.append({
                'season': s,
                'episodes': episodes, 
                'complete': complete,
                'file_ids': ",".join(str(fid) for fid in file_ids_list)
            })

        if not valid_seasons:
             return {'status': 'FAIL', 'reason': f'Stagioni non valide.'}

        canonical = context.canonical_title if hasattr(context, 'canonical_title') else context['canonical_title']
        res = build_series_result(canonical, year, lang_final, metadata, parent_title, valid_seasons)
        return {'status': 'MATCHED', 'info_hash': element['hash'], 'media_type': 'series', 'file_size': sum(f['bytes'] for f in element.get('files', [])), } | res

    return {'status': 'FAIL', 'reason': 'Tipo media sconosciuto'}

def process_element_standard(element, info_hash_map, context):
    parent = info_hash_map.get(element['hash'])
    if not parent:
        return {'status': 'RETRY', 'reason': 'info_hash mancante'}
    return element_validation(element, context, parent)

def process_element_fallback(element, info_hash_map, context):
    """
    Tenta di validare un torrent analizzando i nomi dei file interni o delle cartelle,
    ignorando il titolo del torrent padre (che ha già fallito la validazione standard).
    OTTIMIZZAZIONE: Rimossa dipendenza da pathlib.Path per velocità.
    """
    files = element.get('files', [])
    if not files: return None
    
    candidates = []
    for f in files:
        # Manipolazione stringa pura
        path_str = f['path'].lstrip("/")
        parts = path_str.split('/')
        
        # 1. Analisi Root Folder (parts[0])
        root = parts[0]
        # Check rapido keywords
        if not any(k in root.lower() for k in SKIP_KEYWORDS):
            candidates.append(root)
        
        # 2. Analisi Stem (Nome file senza estensione)
        filename = parts[-1]
        
        # Simulazione di Path.stem usando stringhe
        # Cerchiamo l'ultimo punto per togliere l'estensione
        last_dot_idx = filename.rfind('.')
        if last_dot_idx > 0: # > 0 per evitare file nascosti tipo .gitignore
            stem = filename[:last_dot_idx]
        else:
            stem = filename
            
        if not any(k in stem.lower() for k in SKIP_KEYWORDS):
            candidates.append(stem)
    
    if not candidates: return None
    
    # Selezioniamo il candidato più lungo (spesso il più descrittivo)
    parent = max(candidates, key=len)
    
    res = element_validation(element, context, parent)
    if res['status'] == 'MATCHED':
        logger.info(f"🕵️ [Fallback] Match recuperato: {parent}")
        return res
    
    return None