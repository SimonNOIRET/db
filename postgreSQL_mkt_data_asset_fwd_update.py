import os
import time
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
from scipy.interpolate import PchipInterpolator
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

FOLDER = r'C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data'
TABLE_NAME = "asset_fwd"

FIELDS = {
    "lexifi_forward_id": "lexifi_forward_id",
    "lexifi_id": "lexifi_id",
    "lexifi_forward": "lexifi_forward",
    "lexifi_date": "lexifi_date"
}
TARGET_MATURITIES = list(range(1, 11))
BATCH_SIZE = 500
VACUUM_EVERY = 100

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="lexifi_mkt_data",
    user="postgres",
    password="0112"
)
conn.autocommit = False
cursor = conn.cursor()

csv_files = sorted(
    [f for f in os.listdir(FOLDER) if f.startswith("lexifi_market_data_") and f.endswith(".csv")],
    reverse=True
)

print(f"🔍 {len(csv_files)} fichier(s) trouvé(s). Début du traitement...\n")

file_count = 0
total_start_time = time.time()

for filename in csv_files:
    file_start_time = time.time()
    file_count += 1
    filepath = os.path.join(FOLDER, filename)

    try:
        file_date = datetime.strptime(filename.replace("lexifi_market_data_", "").replace(".csv", ""), "%Y-%m-%d").date()
    except ValueError:
        print(f"⚠️ [{file_count}/{len(csv_files)}] Nom de fichier invalide : {filename}")
        continue

    print(f"📄 [{file_count}/{len(csv_files)}] Traitement de {filename}...")

    try:
        df = pd.read_csv(filepath, header=None, on_bad_lines='skip', low_memory=False)
        df = df[df[0].isin(["Asset_spot", "Asset_forward", "Asset_forward_growth_rate"])]
    except Exception:
        print(f"❌ Erreur lecture fichier : {filename}")
        continue

    df[3] = pd.to_datetime(df[3], format="%Y-%m-%d", errors='coerce').dt.date
    df = df[df[3].notna()].copy()

    spot_prices = {
        str(row[1]).strip(): float(row[2])
        for _, row in df[df[0] == "Asset_spot"].iterrows()
        if str(row[1]).strip() and pd.notna(row[2])
    }

    rows_to_upsert = []

    for df_type, key_name in [
        (df[df[0] == "Asset_forward"], "Asset_forward"),
        (df[df[0] == "Asset_forward_growth_rate"], "Asset_forward_growth_rate")
    ]:
        for market_date, group in df_type.groupby(3):
            curves = {}
            for _, row in group.iterrows():
                try:
                    parts = str(row[1]).strip().split()
                    if len(parts) != 2:
                        continue
                    isin, maturity_str = parts
                    maturity_date = datetime.strptime(maturity_str, "%Y-%m-%d").date()
                    maturity_years = (maturity_date - market_date).days / 365.0
                    if maturity_years <= 0:
                        continue

                    if key_name == "Asset_forward":
                        value = float(row[2])
                    else:
                        growth_rate = float(row[2])
                        spot_price = spot_prices.get(isin, 0)
                        value = spot_price * np.exp(growth_rate * maturity_years)

                    curves.setdefault(isin, []).append((maturity_years, value))
                except Exception:
                    continue

            for isin, points in curves.items():
                if len(points) < 2:
                    continue
                points.sort()
                maturities, values = zip(*points)
                try:
                    interpolator = PchipInterpolator(maturities, values, extrapolate=True)
                except Exception:
                    continue

                for t in TARGET_MATURITIES:
                    try:
                        forward = max(round(float(interpolator(t)), 4), 0.0)
                        lexifi_forward_id = f"{isin} {t}Y"
                        lexifi_id = isin
                        rows_to_upsert.append((lexifi_forward_id, lexifi_id, forward, file_date))
                    except Exception:
                        continue

    try:
        upsert_query = f"""
            INSERT INTO {TABLE_NAME}
            ({FIELDS['lexifi_forward_id']}, {FIELDS['lexifi_id']}, {FIELDS['lexifi_forward']}, {FIELDS['lexifi_date']})
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ({FIELDS['lexifi_forward_id']}, {FIELDS['lexifi_date']})
            DO UPDATE SET
                {FIELDS['lexifi_id']} = EXCLUDED.{FIELDS['lexifi_id']},
                {FIELDS['lexifi_forward']} = EXCLUDED.{FIELDS['lexifi_forward']};
        """

        for i in range(0, len(rows_to_upsert), BATCH_SIZE):
            batch = rows_to_upsert[i:i + BATCH_SIZE]
            cursor.executemany(upsert_query, batch)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"⚠️ Erreur UPSERT : {e}")
        continue

    if file_count % VACUUM_EVERY == 0:
        print("🧼 VACUUM ANALYZE en cours...")
        try:
            conn.commit()  
            conn.autocommit = True  
            cursor.execute(f"VACUUM ANALYZE {TABLE_NAME};")
            conn.autocommit = False  
            print("✅ VACUUM terminé.")
        except Exception as e:
            print(f"⚠️ Erreur VACUUM : {e}")

    file_end_time = time.time()
    print(f"✅ [{file_count}/{len(csv_files)}] {len(rows_to_upsert)} ligne(s) traitée(s) en {file_end_time - file_start_time:.2f} sec.\n")
    time.sleep(0.2)

cursor.close()
conn.close()
total_elapsed_time = time.time() - total_start_time
print(f"🎉 Traitement terminé en {total_elapsed_time:.2f} secondes pour {file_count} fichier(s).")