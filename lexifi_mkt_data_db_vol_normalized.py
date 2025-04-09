import os
import re
import json
import math
import gc
import psycopg2
import numpy as np
from datetime import datetime
from pathlib import Path
from time import time
from scipy.interpolate import CloughTocher2DInterpolator, LinearNDInterpolator
from psycopg2.extras import execute_values

FOLDER = r"C:\\Users\\Simon\\Documents\\ArkeaAM\\VSCode\\Database\\lexifi_mkt_data"
EXT = ".md"
CACHE_DIR = Path(FOLDER) / "cache"
CACHE_DIR.mkdir(exist_ok=True)
CHUNK_SIZE = 500

DB_PARAMS = {
    "dbname": "lexifi_mkt_data",
    "user": "postgres",
    "password": "0112",
    "host": "localhost",
    "port": "5432"
}

RESET = True
INTERPOLATION_METHOD = "clough"  # "clough", "linear"

TABLE_CONFIG = {
    "final": "asset_volatility_normalized",
    "columns": ["lexifi_id", "lexifi_vol_id", "lexifi_vol", "lexifi_date"],
    "keys": ["lexifi_vol_id", "lexifi_date"]
}

def clean_id(entry):
    return re.sub(r"\s+~(extrapolated_volatility)(\s.*)?$", "", entry.strip())

def load_file_cache():
    path = CACHE_DIR / "checksums_volatility_normalized.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_file_cache(cache):
    path = CACHE_DIR / "checksums_volatility_normalized.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def parse_md_file(file_path):
    data = {"Asset_volatility": []}
    with open(file_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or ';' not in line:
                continue
            if line.startswith("Asset_volatility;"):
                data["Asset_volatility"].append(line.replace("Asset_volatility;", "", 1))
    return data

def format_strike(value):
    if value.endswith('%'):
        try:
            numeric = float(value[:-1])
            return round(numeric, 4)
        except ValueError:
            return None
    return None

def interpolate_surface(strikes, ttms, values):
    grid_strikes = np.arange(40.0, 161.0, 10.0)  # 40%, 50%, ..., 160%
    grid_ttms = np.arange(1, 11)  # 1Y to 10Y
    points = np.array(list(zip(ttms, strikes)))
    values = np.array(values)
    try:
        if INTERPOLATION_METHOD == "clough":
            interpolator = CloughTocher2DInterpolator(points, values)
        elif INTERPOLATION_METHOD == "linear":
            interpolator = LinearNDInterpolator(points, values)
        else:
            raise ValueError("Méthode d'interpolation inconnue")

        surface = {}
        for ttm in grid_ttms:
            for strike in grid_strikes:
                vol = interpolator(ttm, strike)
                if np.isnan(vol) or vol <= 0:
                    continue
                surface[(ttm, strike)] = round(float(vol), 6)
        return surface
    except Exception:
        return {}

def process_data(data):
    vols_by_id_date = {}

    for row in data["Asset_volatility"]:
        parts = row.split(';')
        if len(parts) >= 3:
            id_clean = clean_id(parts[0])
            parts_id = id_clean.split()
            if len(parts_id) < 3:
                continue
            lexifi_id, maturity, strike_raw = parts_id[0], parts_id[1], parts_id[2]
            if len(lexifi_id) != 12:
                continue
            strike = format_strike(strike_raw)
            if strike is None:
                continue
            try:
                vol = float(parts[1])
                date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                maturity_date = datetime.strptime(maturity, "%Y-%m-%d").date()
                ttm = (maturity_date - date).days / 365
                if ttm <= 0:
                    continue
                key = (lexifi_id, date)
                vols_by_id_date.setdefault(key, []).append((ttm, strike, vol))
            except Exception:
                continue

    normalized = []
    for (lexifi_id, date), records in vols_by_id_date.items():
        ttms, strikes, vols = zip(*records)
        surface = interpolate_surface(strikes, ttms, vols)
        for (ttm, strike), vol in surface.items():
            vol_id = f"{lexifi_id} {ttm}Y {strike:.2f}%"
            normalized.append((lexifi_id, vol_id, vol, date))

    return normalized

def chunked_insert(cur, rows):
    total = len(rows)
    if total == 0:
        return
    print(f"   ↪ À injecter : {total} dans {TABLE_CONFIG['final']}")
    for i in range(0, total, CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        query = f"INSERT INTO {TABLE_CONFIG['final']} ({', '.join(TABLE_CONFIG['columns'])}) VALUES %s ON CONFLICT DO NOTHING"
        try:
            execute_values(cur, query, chunk)
        except Exception as e:
            print(f"❌ Erreur à l'injection du chunk {i}-{i+CHUNK_SIZE}: {e}")
        if i % (CHUNK_SIZE * 10) == 0 or i + CHUNK_SIZE >= total:
            print(f"      ✅ {min(i + CHUNK_SIZE, total)} / {total}")

def main():
    start = time()
    conn = psycopg2.connect(**DB_PARAMS)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cache = load_file_cache()
    files = sorted(Path(FOLDER).glob(f"*{EXT}"), key=os.path.getmtime)
    new_files = [f for f in files if f.name not in cache or RESET]

    if RESET:
        print(f"♻️  RESET demandé pour asset_volatility_normalized...")
        json_path = CACHE_DIR / "checksums_volatility_normalized.json"
        if json_path.exists():
            os.remove(json_path)
        cur.execute(f"DELETE FROM {TABLE_CONFIG['final']};")
        cur.execute(f"VACUUM ANALYZE {TABLE_CONFIG['final']};")
        cur.execute(f"REINDEX TABLE {TABLE_CONFIG['final']};")

    print(f"\n🔄 ASSET_VOLATILITY_NORMALIZED : {len(new_files)} fichier(s) à traiter")
    total_inserted = 0

    for idx, file in enumerate(new_files, 1):
        print(f"[{idx}/{len(new_files)}] {file.name}")
        parsed = parse_md_file(file)
        rows = process_data(parsed)
        chunked_insert(cur, rows)
        total_inserted += len(rows)
        cache[file.name] = datetime.now().isoformat()
        save_file_cache(cache)
        gc.collect()

    cur.close()
    conn.close()
    print(f"\n✅ Script terminé : {total_inserted} ligne(s) injectée(s) en {round(time() - start, 2)} secondes")

if __name__ == "__main__":
    main()