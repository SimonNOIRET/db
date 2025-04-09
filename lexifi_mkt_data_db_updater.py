import os
import re
import json
import math
import gc
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from pathlib import Path
from time import time

FOLDER = r"C:\\Users\\Simon\\Documents\\ArkeaAM\\VSCode\\Database\\lexifi_mkt_data"
EXT = ".md"
CACHE_DIR = Path(FOLDER) / "cache"
CACHE_DIR.mkdir(exist_ok=True)
DO_VACUUM = True
CHUNK_SIZE = 500

DB_PARAMS = {
    "dbname": "lexifi_mkt_data",
    "user": "postgres",
    "password": "0112",
    "host": "localhost",
    "port": "5432"
}

RESET = {
    "spot": False,
    "forward": False,
    "vol": False
}

TABLES = {
    "spot": {
        "final": "asset_spot",
        "columns": ["lexifi_id", "lexifi_spot", "lexifi_date"],
        "keys": ["lexifi_id", "lexifi_date"]
    },
    "forward": {
        "final": "asset_forward",
        "columns": ["lexifi_id", "lexifi_forward_id", "lexifi_forward", "lexifi_date"],
        "keys": ["lexifi_forward_id", "lexifi_date"]
    },
    "vol": {
        "final": "asset_volatility",
        "columns": ["lexifi_id", "lexifi_vol_id", "lexifi_vol", "lexifi_date"],
        "keys": ["lexifi_vol_id", "lexifi_date"]
    }
}

def clean_id(entry):
    return re.sub(r"\s+~(interpolated_forward|extrapolated_volatility)$", "", entry.strip())

def format_strike(value):
    parts = value.split()
    if len(parts) < 3:
        return value
    strike = parts[-1]
    if strike.endswith('%'):
        try:
            numeric = float(strike[:-1])
            formatted = f"{numeric:.4f}%"
            return ' '.join(parts[:-1] + [formatted])
        except ValueError:
            return value
    return value

def load_file_cache(table):
    path = CACHE_DIR / f"checksums_{table}.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_file_cache(table, cache):
    path = CACHE_DIR / f"checksums_{table}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def parse_md_file(file_path):
    data = {
        "Asset_spot": [],
        "Asset_forward": [],
        "Asset_forward_growth_rate": [],
        "Asset_volatility": []
    }
    with open(file_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or ';' not in line:
                continue
            for key in data:
                if line.startswith(f"{key};"):
                    data[key].append(line.replace(f"{key};", "", 1))
    return data

def chunked_insert(cur, rows, table_config):
    columns = table_config["columns"]
    final = table_config["final"]
    total = len(rows)
    if total == 0:
        return
    print(f"   ↪ À injecter : {total} dans {final}")
    for i in range(0, total, CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        query = f"INSERT INTO {final} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        try:
            execute_values(cur, query, chunk)
        except Exception as e:
            print(f"❌ Erreur à l'injection du chunk {i}-{i+CHUNK_SIZE}: {e}")
        if i % (CHUNK_SIZE * 10) == 0 or i + CHUNK_SIZE >= total:
            print(f"      ✅ {min(i + CHUNK_SIZE, total)} / {total}")

def process_and_insert(table, cur):
    cache = load_file_cache(table)
    files = sorted(Path(FOLDER).glob(f"*{EXT}"), key=os.path.getmtime)
    new_files = [f for f in files if f.name not in cache or RESET[table]]
    total_files = len(new_files)

    print(f"\n🔄 {table.upper()} : {total_files} fichier(s) à traiter")
    total_inserted = 0

    for idx, file in enumerate(new_files, 1):
        print(f"[{idx}/{total_files}] {file.name}")
        parsed = parse_md_file(file)
        rows_spot, rows_forward, rows_vol = process_data([parsed])

        if table == "spot":
            chunked_insert(cur, rows_spot, TABLES["spot"])
            total_inserted += len(rows_spot)
        elif table == "forward":
            chunked_insert(cur, rows_forward, TABLES["forward"])
            total_inserted += len(rows_forward)
        elif table == "vol":
            chunked_insert(cur, rows_vol, TABLES["vol"])
            total_inserted += len(rows_vol)

        cache[file.name] = datetime.now().isoformat()
        save_file_cache(table, cache)

        del parsed, rows_spot, rows_forward, rows_vol
        gc.collect()

    print(f"✅ {table.upper()} terminé : {total_inserted} ligne(s) injectée(s)")

def process_data(all_data):
    spot_dict = {}
    rows_spot = []
    rows_forward = []
    rows_vol = []

    for data in all_data:
        for row in data["Asset_spot"]:
            parts = row.split(';')
            if len(parts) >= 3:
                lexifi_id = parts[0]
                spot = float(parts[1])
                date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                rows_spot.append((lexifi_id, spot, date))
                spot_dict[(lexifi_id, date)] = spot

        for row in data["Asset_forward"]:
            parts = row.split(';')
            if len(parts) >= 3:
                id_date = clean_id(parts[0])
                lexifi_id, maturity = id_date.split()
                forward_id = f"{lexifi_id} {maturity}"
                forward_val = float(parts[1])
                date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                rows_forward.append((lexifi_id, forward_id, forward_val, date))

        for row in data["Asset_forward_growth_rate"]:
            parts = row.split(';')
            if len(parts) >= 3:
                id_date = clean_id(parts[0])
                lexifi_id, maturity = id_date.split()
                growth_rate = float(parts[1])
                date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                maturity_date = datetime.strptime(maturity, "%Y-%m-%d").date()
                T = (maturity_date - date).days / 365
                spot = spot_dict.get((lexifi_id, date))
                if spot:
                    forward_val = spot * math.exp(growth_rate * T)
                    forward_id = f"{lexifi_id} {maturity}"
                    rows_forward.append((lexifi_id, forward_id, round(forward_val, 6), date))

        for row in data["Asset_volatility"]:
            parts = row.split(';')
            if len(parts) >= 3:
                raw_id = clean_id(parts[0])
                formatted_id = format_strike(raw_id)
                lexifi_id = raw_id.split()[0]
                vol_val = float(parts[1])
                date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                rows_vol.append((lexifi_id, formatted_id, round(vol_val, 6), date))

    return rows_spot, rows_forward, rows_vol

def vacuum_and_reindex_table(cur, table_name):
    print(f"🧹 VACUUM + REINDEX {table_name}...")
    cur.execute(f"VACUUM ANALYZE {table_name};")
    cur.execute(f"REINDEX TABLE {table_name};")
    print(f"   ✅ {table_name} nettoyée")

def main():
    start = time()
    conn = psycopg2.connect(**DB_PARAMS)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    for table in TABLES:
        if RESET[table]:
            print(f"♻️  RESET demandé pour {table.upper()}...")
            cache_path = CACHE_DIR / f"checksums_{table}.json"
            if cache_path.exists():
                os.remove(cache_path)
            cur.execute(f"DELETE FROM {TABLES[table]['final']};")
            vacuum_and_reindex_table(cur, TABLES[table]['final'])

        process_and_insert(table, cur)

    if DO_VACUUM:
        for conf in TABLES.values():
            vacuum_and_reindex_table(cur, conf['final'])

    cur.close()
    conn.close()
    print(f"\n✅ Script terminé en {round(time() - start, 2)} secondes")

if __name__ == "__main__":
    main()