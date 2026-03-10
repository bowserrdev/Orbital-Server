from config import get_logger
from validation import process_element_standard, process_element_fallback
from services import SearchService
from utils import res_rank

logger = get_logger("Processing")

def cpu_process_batch(
    chunk: list, 
    info_hash_map: dict, 
    context, 
    profiles: list, 
    global_limits: dict,
    req_user_id: int,
    req_season: int | None = None,
    req_ep: int | None = None
) -> list[dict]:
    """
    Funzione Pura (Stateless) eseguita dai Worker (Core 1-3).
    
    Responsabilità:
    1. Parsing e Validazione (Standard + Fallback).
    2. Filtraggio Hardware globale (Max Res, Max Size).
    3. Calcolo Score per TUTTI i profili (Single Pass).
    4. Preparazione metadati per Fast Lane (per alleggerire il Main).
    
    Returns:
        Lista di dizionari "arricchiti" pronti per DB e Risposta API.
    """
    processed_items = []
    
    # Pre-calcolo limiti per velocità
    max_res_rank = global_limits.get('max_res', 999)
    max_size_bytes = global_limits.get('max_size', 0)

    for item in chunk:
        try:
            # ==============================================================================
            # 1. PARSING & VALIDAZIONE (CPU Intensive)
            # ==============================================================================
            
            # Tentativo Standard
            res = process_element_standard(item, info_hash_map, context)
            
            # Tentativo Fallback (se Standard fallisce)
            if not res or res.get('status') != 'MATCHED':
                res = process_element_fallback(item, info_hash_map, context)
            
            # Se ancora fallito, scarta
            if not res or res.get('status') != 'MATCHED':
                continue

            # ==============================================================================
            # 2. FILTRI HARDWARE GLOBALI
            # ==============================================================================
            
            item_res_rank = res_rank(res.get('resolution', 'Unknown'))
            
            # Scarta se risoluzione troppo alta per il server
            if item_res_rank > max_res_rank:
                continue
                
            # Scarta se file troppo grande (se limite impostato)
            if max_size_bytes > 0 and res['file_size'] > max_size_bytes:
                continue

            # ==============================================================================
            # 3. NORMALIZZAZIONE DATI
            # ==============================================================================
            
            # Aggiungiamo campi essenziali per il DB e il Routing
            res['imdb_id'] = context.imdb_id
            if 'info_hash' not in res: 
                res['info_hash'] = res['hash']

            # ==============================================================================
            # 4. SCORING MULTI-PROFILO (Single Pass)
            # ==============================================================================
            
            # Calcoliamo lo score per ogni profilo esistente.
            # Questo permette al DbWriter di salvare solo il meglio per chiunque,
            # e al Main di rispondere subito all'utente richiedente.
            
            scores = {}
            for profile in profiles:
                # Calcolo score puro
                score = SearchService.calculate_score(res, profile)
                scores[profile.id] = score
            
            # Alleghiamo la mappa degli score all'oggetto
            res['_scores'] = scores

            # ==============================================================================
            # 5. FAST LANE PRE-CALCULATION (Specifico per l'utente richiedente)
            # ==============================================================================
            # Qui prepariamo i dati "pronti all'uso" per la risposta API immediata,
            # così il Core 0 (Main) non deve iterare liste di episodi.
            
            req_score = scores.get(req_user_id, -1000)
            
            # Identifichiamo il profilo richiedente per la soglia
            req_profile = next((p for p in profiles if p.id == req_user_id), None)
            min_threshold = req_profile.min_score_threshold if req_profile else 0
            
            # Se il punteggio è sufficiente per l'utente richiedente, analizziamo se il contenuto è quello giusto
            is_fast_lane_candidate = False
            fast_lane_data = None

            if req_score >= min_threshold:
                if res['media_type'] == 'series':
                    # Logica specifica Serie: Cerchiamo se c'è la stagione/episodio richiesto
                    found_season_data = None
                    seasons_data = res.get('seasons_data', [])
                    
                    for s_entry in seasons_data:
                        # Filtro Stagione
                        if req_season is not None and s_entry['season'] != req_season: 
                            continue
                        
                        is_good = False
                        # Se è season pack completo
                        if s_entry['complete']: 
                            is_good = True
                        # Se contiene l'episodio specifico
                        elif req_ep is not None and req_ep in s_entry['episodes']:
                            is_good = True
                        
                        # Se abbiamo trovato un match valido per la richiesta
                        if is_good:
                            found_season_data = s_entry
                            break
                    
                    if found_season_data:
                        # Creiamo un subset di dati pulito per la risposta API
                        # (Sonarr non vuole tutto il JSON nested, vuole i dati specifici)
                        is_fast_lane_candidate = True
                        fast_lane_data = {
                            'arr_title': found_season_data['arr_title'],
                            'file_ids': found_season_data['file_ids'],
                            'season': found_season_data['season'],
                            'episodes': found_season_data['episodes'], # Set o List
                            'info_hash': res['info_hash'],
                            'title': res['title'],
                            'file_size': res['file_size'],
                            'imdb_id': res['imdb_id'],
                            'media_type': 'series',
                            # Importante: passiamo lo score calcolato
                            '_score': req_score 
                        }
                else:
                    # Logica Movie: è sempre valido se lo score è alto
                    is_fast_lane_candidate = True
                    fast_lane_data = res.copy()
                    # Pulizia per sicurezza (rimuovi dati pesanti se ce ne fossero)
                    if 'seasons_data' in fast_lane_data: del fast_lane_data['seasons_data']
                    fast_lane_data['_score'] = req_score

            # Alleghiamo i metadati Fast Lane all'oggetto principale
            res['_fast_lane_ready'] = is_fast_lane_candidate
            if is_fast_lane_candidate:
                res['_fast_lane_payload'] = fast_lane_data

            processed_items.append(res)

        except Exception as e:
            # Logghiamo l'errore ma non fermiamo il batch
            # Usiamo hash parziale o 'unknown' per identificare l'item problematico
            h = item.get('hash', 'unknown')
            logger.error(f"Errore processing item {h}: {e}", exc_info=False)
            continue

    return processed_items