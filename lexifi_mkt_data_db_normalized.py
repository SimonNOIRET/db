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
from scipy.interpolate import PchipInterpolator, interp1d, LSQUnivariateSpline
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

RESET = True  # uniquement asset_forward_normalized
INTERPOLATION_METHOD = "pchip"  # "pchip", "nspline", "linear" (fallback inclus)

TABLE_CONFIG = {
    "final": "asset_forward_normalized",
    "columns": ["lexifi_id", "lexifi_forward_id", "lexifi_forward", "lexifi_date"],
    "keys": ["lexifi_forward_id", "lexifi_date"]
}

def clean_id(entry):
    return re.sub(r"\s+~(interpolated_forward|extrapolated_volatility)(\s.*)?$", "", entry.strip())

def load_file_cache():
    path = CACHE_DIR / "checksums_forward_normalized.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_file_cache(cache):
    path = CACHE_DIR / "checksums_forward_normalized.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def parse_md_file(file_path):
    data = {"Asset_forward": [], "Asset_forward_growth_rate": [], "Asset_spot": []}
    with open(file_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or ';' not in line:
                continue
            for key in data:
                if line.startswith(f"{key};"):
                    data[key].append(line.replace(f"{key};", "", 1))
    return data

def interpolate_forward(ttms, values):
    grid = np.arange(1, 11)
    try:
        if INTERPOLATION_METHOD == "nspline" and len(ttms) >= 4:
            try:
                knots = np.linspace(ttms[1], ttms[-2], len(ttms) - 2)
                spline = LSQUnivariateSpline(ttms, values, knots)
                out = spline(grid)
                if np.any(out <= 0):
                    raise ValueError("Negative output")
                return dict(zip(grid, out))
            except Exception:
                pass
        if INTERPOLATION_METHOD in ["pchip", "nspline"]:
            try:
                interp = PchipInterpolator(ttms, values, extrapolate=True)
                out = interp(grid)
                if np.any(out <= 0):
                    raise ValueError("Negative output")
                return dict(zip(grid, out))
            except Exception:
                pass
        fallback = interp1d(ttms, values, kind="linear", fill_value="extrapolate")
        out = fallback(grid)
        if np.any(out <= 0):
            raise ValueError("Negative output")
        return dict(zip(grid, out))
    except Exception:
        return {}

def process_data(data):
    forwards = {}
    spot_cache = {}

    for row in data["Asset_spot"]:
        parts = row.split(';')
        if len(parts) >= 3:
            lexifi_id = parts[0].strip()
            if len(lexifi_id) != 12:
                continue
            try:
                spot_val = float(parts[1])
                date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                spot_cache[(lexifi_id, date)] = spot_val
            except Exception:
                continue

    for row in data["Asset_forward"]:
        parts = row.split(';')
        if len(parts) >= 3:
            id_date = clean_id(parts[0])
            parts_id = id_date.split()
            if len(parts_id) < 2:
                continue
            lexifi_id, maturity = parts_id[0], parts_id[1]
            if len(lexifi_id) != 12:
                continue
            date = datetime.strptime(parts[2], "%Y-%m-%d").date()
            ttm = (datetime.strptime(maturity, "%Y-%m-%d").date() - date).days / 365
            key = (lexifi_id, date)
            forwards.setdefault(key, []).append((ttm, float(parts[1])))

    for row in data["Asset_forward_growth_rate"]:
        parts = row.split(';')
        if len(parts) >= 3:
            id_date = clean_id(parts[0])
            parts_id = id_date.split()
            if len(parts_id) < 2:
                continue
            lexifi_id, maturity = parts_id[0], parts_id[1]
            if len(lexifi_id) != 12:
                continue
            date = datetime.strptime(parts[2], "%Y-%m-%d").date()
            maturity_date = datetime.strptime(maturity, "%Y-%m-%d").date()
            T = (maturity_date - date).days / 365
            if T <= 0:
                continue
            spot = spot_cache.get((lexifi_id, date))
            if spot is None:
                continue
            fwd = spot * math.exp(float(parts[1]) * T)
            forwards.setdefault((lexifi_id, date), []).append((T, fwd))

    normalized = []
    for (lexifi_id, date), points in forwards.items():
        points.sort()
        ttms, values = zip(*points)
        curve = interpolate_forward(np.array(ttms), np.array(values))
        for ttm_year, price in curve.items():
            forward_id = f"{lexifi_id} {ttm_year}Y"
            normalized.append((lexifi_id, forward_id, round(float(price), 6), date))

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
        print(f"♻️  RESET demandé pour asset_forward_normalized...")
        json_path = CACHE_DIR / "checksums_forward_normalized.json"
        if json_path.exists():
            os.remove(json_path)
        cur.execute(f"DELETE FROM {TABLE_CONFIG['final']};")
        cur.execute(f"VACUUM ANALYZE {TABLE_CONFIG['final']};")
        cur.execute(f"REINDEX TABLE {TABLE_CONFIG['final']};")

    print(f"\n🔄 ASSET_FORWARD_NORMALIZED : {len(new_files)} fichier(s) à traiter")
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