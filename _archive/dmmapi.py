import asyncio
import time
import math
import secrets
import random
import httpx

# --- CONFIGURAZIONE ---
BASE_URL = "https://debridmediamanager.com"

# Header "mimetici" presi dal browser
BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "it",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Priority": "u=4",
    "TE": "trailers",
    "Origin": "https://debridmediamanager.com",
    "Content-Type": "application/json",
}

class DMMCrypto:
    SALT = "debridmediamanager.com%%fe7#td00rA3vHz%VmI"

    @staticmethod
    def _js_imul(a, b): return (a * b) & 0xFFFFFFFF
    @staticmethod
    def _urshift(val, n): return (val & 0xFFFFFFFF) >> n
    @staticmethod
    def _hash_func(input_str):
        i = 0xdeadbeef ^ len(input_str)
        t = 0x41c6ce57 ^ len(input_str)
        for char in input_str:
            l = ord(char)
            prev_i, prev_t = i, t
            xor_res = (prev_i ^ l)
            imul_res = DMMCrypto._js_imul(xor_res, 0x9e3779b1)
            i = ((imul_res << 5) & 0xFFFFFFFF | DMMCrypto._urshift(imul_res, 27)) & 0xFFFFFFFF
            xor_res_t = (prev_t ^ l)
            imul_res_t = DMMCrypto._js_imul(xor_res_t, 0x5f356495)
            t = ((imul_res_t << 5) & 0xFFFFFFFF | DMMCrypto._urshift(imul_res_t, 27)) & 0xFFFFFFFF
        i = (i + DMMCrypto._js_imul(t, 0x5d588b65)) & 0xFFFFFFFF
        t = (t + DMMCrypto._js_imul(i, 0x78a76a79)) & 0xFFFFFFFF
        return format(DMMCrypto._urshift(i ^ t, 0), 'x')

    @staticmethod
    def solve():
        rand_int = secrets.randbits(32)
        random_hex = format(rand_int, 'x')
        timestamp = int(time.time())
        key = f"{random_hex}-{timestamp}"
        hash_a = DMMCrypto._hash_func(key)
        hash_b = DMMCrypto._hash_func(f"{DMMCrypto.SALT}-{random_hex}")
        length_a = len(hash_a)
        half_len = math.floor(length_a / 2)
        interleaved = "".join([hash_a[x] + hash_b[x] for x in range(half_len)])
        return key, interleaved + hash_b[half_len:][::-1] + hash_a[half_len:][::-1]

class DMMApi:
    def __init__(self):
        self.client = httpx.AsyncClient(
            headers=BASE_HEADERS, 
            timeout=45.0, 
            follow_redirects=True
        )

    async def _request(self, method, url, **kwargs):
        retries = 3
        for attempt in range(retries):
            try:
                if method == 'GET': resp = await self.client.get(url, **kwargs)
                else: resp = await self.client.post(url, **kwargs)
                
                if resp.status_code == 429:
                    wait = 2.0 + (attempt * 1.5)
                    await asyncio.sleep(wait)
                    continue
                
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429: continue
                print(f"❌ [API] HTTP {e.response.status_code}: {e}")
                return None
            except Exception as e:
                print(f"❌ [API] Err: {e}")
                return None
        return None

    async def search_with_availability(self, imdb_id, media_type="movie", season=None, max_pages=3):
        """
        Generatore: Scarica pagina -> Controlla Cache -> Restituisce Batch
        """
        page = 0
        
        # 1. MAPPING ENDPOINT API E URL
        # Torznab ci passa "series", ma l'API DMM vuole "tv"
        # Il Frontend DMM usa "show" per le serie
        if media_type == "series" or media_type == "tv":
            api_endpoint = "tv"
            frontend_type = "show"
        else:
            api_endpoint = "movie"
            frontend_type = "movie"
        
        base_api_url = f"{BASE_URL}/api/torrents/{api_endpoint}"

        # 2. COSTRUZIONE REFERER (Imita il browser)
        # Url Frontend: https://debridmediamanager.com/show/tt12345/1
        ref_url = f"{BASE_URL}/{frontend_type}/{imdb_id}"
        if api_endpoint == "tv" and season:
            ref_url += f"/{season}"
        
        req_headers = BASE_HEADERS.copy()
        req_headers["Referer"] = ref_url

        while page < max_pages:
            key, solution = DMMCrypto.solve()
            
            # 3. COSTRUZIONE PARAMETRI
            params = {
                "imdbId": imdb_id,
                "dmmProblemKey": key,
                "solution": solution,
                "onlyTrusted": "false",
                "maxSize": 0,
                "page": page
            }
            
            # FIX: Per le serie TV, DMM vuole "seasonNum" nei parametri query
            if api_endpoint == "tv" and season is not None:
                params["seasonNum"] = season

            # --- SEARCH (GET) ---
            data = await self._request('GET', base_api_url, params=params, headers=req_headers)
            
            raw_results = data.get("results", []) if data else []
            if not raw_results: break 

            valid_items = [i for i in raw_results if i.get('hash')]
            if not valid_items: break

            # --- CHECK (POST) ---
            hashes_to_check = [i['hash'] for i in valid_items]
            
            k_post, s_post = DMMCrypto.solve()
            payload = {"imdbId": imdb_id, "hashes": hashes_to_check, "dmmProblemKey": k_post, "solution": s_post}
            
            chk_data = await self._request('POST', f"{BASE_URL}/api/availability/check", json=payload, headers=req_headers)
            avail_list = chk_data.get("available", []) if chk_data else []
            
            # --- MERGE ---
            avail_map = {i['hash']: i for i in avail_list}
            batch = []
            
            for item in valid_items:
                h = item['hash']
                if h in avail_map:
                    merged = {**item, **avail_map[h]}
                    batch.append(merged)

            yield batch
            
            page += 1
            await asyncio.sleep(random.uniform(1.5, 2.0))

    async def close(self):
        await self.client.aclose()