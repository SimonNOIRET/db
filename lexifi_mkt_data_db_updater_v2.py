import os
import re
import json
import math
import psycopg2
import tempfile
from datetime import datetime
from pathlib import Path
from time import time

FOLDER = r"C:\\Users\\Simon\\Documents\\ArkeaAM\\VSCode\\lexifi_mkt_data"
EXT = ".md"
CACHE_FILE = "checksums_db.json"
DO_VACUUM = False

DB_PARAMS = {
    "dbname": "lexifi_mkt_data",
    "user": "postgres",
    "password": "0112",
    "host": "localhost",
    "port": "5432"
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

def load_file_cache():
    if Path(CACHE_FILE).exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_file_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
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
            if line.startswith("Asset_spot;"):
                data["Asset_spot"].append(line.replace("Asset_spot;", "", 1))
            elif line.startswith("Asset_forward;"):
                data["Asset_forward"].append(line.replace("Asset_forward;", "", 1))
            elif line.startswith("Asset_forward_growth_rate;"):
                data["Asset_forward_growth_rate"].append(line.replace("Asset_forward_growth_rate;", "", 1))
            elif line.startswith("Asset_volatility;"):
                data["Asset_volatility"].append(line.replace("Asset_volatility;", "", 1))
    return data

def process_data(all_data):
    spot_dict = {}
    rows_spot = set()
    rows_forward = set()
    rows_vol = set()

    for data in all_data:
        for row in data["Asset_spot"]:
            parts = row.split(';')
            if len(parts) >= 3:
                lexifi_id = parts[0]
                spot = float(parts[1])
                date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                rows_spot.add((lexifi_id, spot, date))
                spot_dict[(lexifi_id, date)] = spot

        for row in data["Asset_forward"]:
            parts = row.split(';')
            if len(parts) >= 3:
                id_date = clean_id(parts[0])
                lexifi_id, maturity = id_date.split()
                forward_id = f"{lexifi_id} {maturity}"
                forward_val = float(parts[1])
                date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                rows_forward.add((lexifi_id, forward_id, forward_val, date))

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
                    rows_forward.add((lexifi_id, forward_id, round(forward_val, 6), date))

        for row in data["Asset_volatility"]:
            parts = row.split(';')
            if len(parts) >= 3:
                raw_id = clean_id(parts[0])
                formatted_id = format_strike(raw_id)
                lexifi_id = raw_id.split()[0]
                vol_val = float(parts[1])
                date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                rows_vol.add((lexifi_id, formatted_id, round(vol_val, 6), date))

    return list(rows_spot), list(rows_forward), list(rows_vol)

def insert_with_staging(cur, rows, config):
    if not rows:
        return
    staging = f"staging_{config['final']}"
    columns = config["columns"]
    keys = config["keys"]

    cur.execute(f"DROP TABLE IF EXISTS {staging};")
    col_defs = ", ".join([
        f"{col} NUMERIC(15,6)" if "spot" in col or "forward" in col or "vol" in col else
        f"{col} DATE" if "date" in col else f"{col} TEXT"
        for col in columns
    ])
    cur.execute(f"CREATE UNLOGGED TABLE {staging} ({col_defs});")

    tmp_path = Path("C:/Users/Simon/Documents/ArkeaAM/VSCode/temp.tsv")
    with open(tmp_path, 'w', encoding='utf-8') as tmpfile:
        for row in rows:
            tmpfile.write("\t".join(str(x) for x in row) + "\n")

    with open(tmp_path, 'r', encoding='utf-8') as f:
        cur.copy_from(f, staging, sep='\t', columns=columns)

    on_clause = " AND ".join([f"f.{k} = s.{k}" for k in keys])
    isnull = " AND ".join([f"f.{k} IS NULL" for k in keys])

    cur.execute(f"""
        INSERT INTO {config['final']} ({', '.join(columns)})
        SELECT {', '.join('s.' + col for col in columns)}
        FROM {staging} s
        LEFT JOIN {config['final']} f ON {on_clause}
        WHERE {isnull};
    """)

    try:
        os.remove(tmp_path)
    except Exception as e:
        print(f"⚠️ Erreur suppression du fichier temporaire: {e}")

def vacuum_and_reindex(cur):
    for conf in TABLES.values():
        table = conf['final']
        print(f"🧹 VACUUM ANALYZE {table}...")
        cur.execute(f"VACUUM ANALYZE {table};")
        cur.execute(f"REINDEX TABLE {table};")
        print(f"   ✅ {table} réindexée")

def main():
    start = time()
    conn = psycopg2.connect(**DB_PARAMS)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cache = load_file_cache()
    files = sorted(Path(FOLDER).glob(f"*{EXT}"), key=os.path.getmtime)
    new_files = [f for f in files if f.name not in cache]
    total_files = len(new_files)
    print(f"📂 {total_files} fichier(s) à traiter")

    all_data = []
    for idx, file in enumerate(new_files, 1):
        print(f"[{idx}/{total_files}] 🔍 Lecture de {file.name}...")
        parsed = parse_md_file(file)
        all_data.append(parsed)
        cache[file.name] = datetime.now().isoformat()
        total = sum(len(parsed[k]) for k in parsed)
        print(f"   ✅ {file.name} : {total} ligne(s) détectée(s)")

    rows_spot, rows_forward, rows_vol = process_data(all_data)
    print(f"📤 Spots à insérer : {len(rows_spot)}")
    print(f"📤 Forwards à insérer : {len(rows_forward)}")
    print(f"📤 Volatilités à insérer : {len(rows_vol)}")

    insert_with_staging(cur, rows_spot, TABLES["spot"])
    insert_with_staging(cur, rows_forward, TABLES["forward"])
    insert_with_staging(cur, rows_vol, TABLES["vol"])

    if DO_VACUUM:
        print("🛠 VACUUM & REINDEX activés...")
        vacuum_and_reindex(cur)

    save_file_cache(cache)

    cur.close()
    conn.close()
    print(f"✅ Script terminé en {round(time() - start, 2)} secondes")

if __name__ == "__main__":
    main()