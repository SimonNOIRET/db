import os
import re
import math
import psycopg2
from datetime import datetime
from pathlib import Path
from io import StringIO
from time import time

FOLDER = r"C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data"
LOG_FILE = "log_lignes_ignorees.txt"

DB_PARAMS = {
    "dbname": "lexifi_mkt_data",
    "user": "postgres",
    "password": "0112",
    "host": "localhost",
    "port": "5432"
}

TABLES = {
    "spot": {
        "table": "asset_spot",
        "columns": ["lexifi_id", "lexifi_spot", "lexifi_date"],
        "conflict": "(lexifi_id, lexifi_date)"
    },
    "forward": {
        "table": "asset_forward",
        "columns": ["lexifi_id", "lexifi_forward_id", "lexifi_forward", "lexifi_date"],
        "conflict": "(lexifi_forward_id, lexifi_date)"
    },
    "vol": {
        "table": "asset_volatility",
        "columns": ["lexifi_id", "lexifi_vol_id", "lexifi_vol", "lexifi_date"],
        "conflict": "(lexifi_vol_id, lexifi_date)"
    }
}

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

def process_data(all_data, conn):
    spot_dict = {}
    rows_spot = set()
    rows_forward = set()
    rows_vol = set()

    log_lines = []

    print(f"   ➔ Traitement de {len(all_data)} fichier(s)...")

    for idx, data in enumerate(all_data, 1):
        print(f"     • Prétraitement fichier {idx}/{len(all_data)}")

        for row in data["Asset_spot"]:
            parts = row.split(';')
            if len(parts) >= 3:
                try:
                    lexifi_id = parts[0]
                    spot = float(parts[1])
                    date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                    rows_spot.add((lexifi_id, spot, date))
                    spot_dict[(lexifi_id, date)] = spot
                except Exception as e:
                    log_lines.append(f"[SPOT] Ligne ignorée: {row} — Erreur: {e}")
            else:
                log_lines.append(f"[SPOT] Ligne ignorée (format insuffisant): {row}")

        for row in data["Asset_forward"]:
            parts = row.split(';')
            if len(parts) >= 3:
                try:
                    id_date = clean_id(parts[0])
                    lexifi_id, maturity = id_date.split()
                    forward_val = float(parts[1])
                    date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                    forward_id = f"{lexifi_id} {maturity}"
                    rows_forward.add((lexifi_id, forward_id, forward_val, date))
                except Exception as e:
                    log_lines.append(f"[FORWARD] Ligne ignorée: {row} — Erreur: {e}")
            else:
                log_lines.append(f"[FORWARD] Ligne ignorée (format insuffisant): {row}")

        for row in data["Asset_forward_growth_rate"]:
            parts = row.split(';')
            if len(parts) >= 3:
                try:
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
                except Exception as e:
                    log_lines.append(f"[GROWTH] Ligne ignorée: {row} — Erreur: {e}")
            else:
                log_lines.append(f"[GROWTH] Ligne ignorée (format insuffisant): {row}")

        for row in data["Asset_volatility"]:
            parts = row.split(';')
            if len(parts) >= 3:
                try:
                    raw_id = clean_id(parts[0])
                    formatted_id = format_strike(raw_id)
                    lexifi_id = raw_id.split()[0]
                    vol_val = float(parts[1])
                    date = datetime.strptime(parts[2], "%Y-%m-%d").date()
                    rows_vol.add((lexifi_id, formatted_id, round(vol_val, 6), date))
                except Exception as e:
                    log_lines.append(f"[VOL] Ligne ignorée: {row} — Erreur: {e}")
            else:
                log_lines.append(f"[VOL] Ligne ignorée (format insuffisant): {row}")

    if log_lines:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(log_lines))
            print(f"⚠️ {len(log_lines)} ligne(s) ignorée(s) — voir {LOG_FILE}")

    return list(rows_spot), list(rows_forward), list(rows_vol)

def copy_to_temp_and_insert(conn, rows, config):
    table_name = config["table"]
    columns = config["columns"]
    conflict = config["conflict"]

    cur = conn.cursor()
    temp_table = f"temp_{table_name}"

    column_defs = []
    for col in columns:
        if col in ["lexifi_spot", "lexifi_forward", "lexifi_vol"]:
            col_type = "NUMERIC(15,6)"
        elif "date" in col:
            col_type = "DATE"
        else:
            col_type = "TEXT"
        column_defs.append(f"{col} {col_type}")

    cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
    cur.execute(f"""
        CREATE TEMP TABLE {temp_table} (
            {', '.join(column_defs)}
        ) ON COMMIT DROP;
    """)

    buffer = StringIO()
    for row in rows:
        buffer.write('\t'.join(str(x) for x in row) + '\n')
    buffer.seek(0)

    cur.copy_from(buffer, temp_table, sep='\t', columns=columns)

    col_list = ', '.join(columns)
    cur.execute(f"""
        INSERT INTO {table_name} ({col_list})
        SELECT {col_list} FROM {temp_table}
        ON CONFLICT {conflict} DO NOTHING;
    """)

    cur.close()

def vacuum_tables(verbose=False):
    vacuum_conn = psycopg2.connect(**DB_PARAMS)
    vacuum_conn.set_session(autocommit=True)
    cur = vacuum_conn.cursor()
    for table in [TABLES["spot"]["table"], TABLES["forward"]["table"], TABLES["vol"]["table"]]:
        if verbose:
            print(f"🩹 VACUUM ANALYZE {table}...")
        t0 = time()
        cur.execute(f"VACUUM ANALYZE {table};")
        cur.execute(f"REINDEX TABLE {table};")
        print(f"   ⏳ Terminé en {round(time() - t0, 2)} sec")
    cur.close()
    vacuum_conn.close()

def extract_date_from_filename(filename):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    return datetime.strptime(match.group(1), "%Y-%m-%d") if match else datetime.min

def main():
    start_time = time()

    files = sorted(
        Path(FOLDER).glob("lexifi_market_data_*.md"),
        key=lambda f: extract_date_from_filename(f.name),
        reverse=True
    )

    total_files = len(files)
    print(f"📂 {total_files} fichier(s) détecté(s)")

    conn = psycopg2.connect(**DB_PARAMS)

    print("🛠 Entretien initial : VACUUM + REINDEX des tables...")
    vacuum_tables(verbose=True)

    all_data = []
    total_spot = 0
    total_forward = 0
    total_vol = 0

    for idx, file_path in enumerate(files, 1):
        print(f"\n📝 [{idx}/{total_files}] {file_path.name}")
        md_data = parse_md_file(file_path)
        all_data.append(md_data)

        spot_count = len(md_data["Asset_spot"])
        forward_count = len(md_data["Asset_forward"]) + len(md_data["Asset_forward_growth_rate"])
        vol_count = len(md_data["Asset_volatility"])
        print(f"   ├─ {spot_count} spot(s)")
        print(f"   ├─ {forward_count} forward(s)")
        print(f"   └─ {vol_count} volatilité(s)")
        total_spot += spot_count
        total_forward += forward_count
        total_vol += vol_count

    if all_data:
        print("\n🔄 Prétraitement et préfiltrage en mémoire...")
        rows_spot, rows_forward, rows_vol = process_data(all_data, conn)

        print(f"🔎 Spots soumis à l'insertion : {len(rows_spot)} / {total_spot}")
        print(f"🔎 Forwards soumis à l'insertion : {len(rows_forward)} / {total_forward}")
        print(f"🔎 Volatilités soumises à l'insertion : {len(rows_vol)} / {total_vol}")

        if rows_spot:
            print("📤 Insertion des spots en base...")
            copy_to_temp_and_insert(conn, rows_spot, TABLES["spot"])
            print(f"   ✅ {len(rows_spot)} spot(s) proposé(s) à l'insertion")

        if rows_forward:
            print("📤 Insertion des forwards en base...")
            copy_to_temp_and_insert(conn, rows_forward, TABLES["forward"])
            print(f"   ✅ {len(rows_forward)} forward(s) proposé(s) à l'insertion")

        if rows_vol:
            print("📤 Insertion des volatilités en base...")
            copy_to_temp_and_insert(conn, rows_vol, TABLES["vol"])
            print(f"   ✅ {len(rows_vol)} volatilité(s) proposée(s) à l'insertion")

        conn.commit()
        print("✅ Insertion validée")
    else:
        print("✅ Toutes les données sont déjà à jour.")

    print("\n🛠 Post-traitement : VACUUM + REINDEX après insertion...")
    vacuum_tables(verbose=True)

    conn.close()
    print(f"\n⏱️ Script terminé en {round(time() - start_time, 2)} secondes")

if __name__ == "__main__":
    main()

