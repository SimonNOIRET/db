import os
import time
import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime
from scipy.interpolate import PchipInterpolator
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

FOLDER = r'C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data'
DB_PATH = r'C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data.accdb'
TABLE_NAME = "asset_fwd"
FIELDS = {
    "lexifi_forward_id": "lexifi_forward_id",  # ISIN + maturité
    "lexifi_id": "lexifi_id",  # ISIN uniquement
    "lexifi_forward": "lexifi_forward",  # Valeur interpolée
    "lexifi_date": "lexifi_date"  # Date du cours
}
TARGET_MATURITIES = list(range(1, 11))  # 1Y à 10Y

conn_str = rf'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={DB_PATH};'
conn = pyodbc.connect(conn_str, autocommit=False)
cursor = conn.cursor()

existing_rows = set()
query = f"SELECT {FIELDS['lexifi_forward_id']}, {FIELDS['lexifi_forward']}, {FIELDS['lexifi_date']} FROM {TABLE_NAME}"
cursor.execute(query)
existing_rows.update((str(row[0]), f"{float(row[1]):.4f}", row[2].date() if isinstance(row[2], datetime) else row[2]) for row in cursor.fetchall())

csv_files = sorted([f for f in os.listdir(FOLDER) if f.startswith("lexifi_market_data_") and f.endswith(".csv")], reverse=True)
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
        print(f"⚠️ [{file_count}/{len(csv_files)}] Fichier ignoré (nom invalide) : {filename}")
        continue

    print(f"📄 [{file_count}/{len(csv_files)}] Traitement de {filename} (date marché : {file_date})...")

    try:
        # Charger le fichier avec gestion des erreurs et filtrer les lignes utiles
        df = pd.read_csv(filepath, header=None, on_bad_lines='skip', low_memory=False)

        # Garder uniquement les lignes "Asset_spot", "Asset_forward", "Asset_forward_growth_rate"
        df = df[df[0].isin(["Asset_spot", "Asset_forward", "Asset_forward_growth_rate"])]
    except Exception:
        print(f"❌ [{file_count}/{len(csv_files)}] Erreur lecture fichier : {filename}")
        continue

    # Correction des dates (colonne 3)
    df[3] = pd.to_datetime(df[3], format="%Y-%m-%d", errors='coerce').dt.date
    df = df[df[3].notna()].copy()

    # Extraction des prix spot
    spot_prices = {str(row[1]).strip(): float(row[2]) for _, row in df[df[0] == "Asset_spot"].iterrows() if str(row[1]).strip() and pd.notna(row[2])}

    rows_to_insert = []
    for df_type, key_name in [(df[df[0] == "Asset_forward"], "Asset_forward"), (df[df[0] == "Asset_forward_growth_rate"], "Asset_forward_growth_rate")]:
        for market_date, group in df_type.groupby(3):  # Group by date
            curves = {}
            for _, row in group.iterrows():
                try:
                    parts = str(row[1]).strip().split()
                    if len(parts) != 2:
                        continue
                    isin, maturity_str = parts

                    # Correction : format explicite pour la date en YYYY-MM-DD
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
                        forward_str = f"{forward:.4f}"
                        lexifi_forward_id = f"{isin} {t}Y"  # Correction : ISIN + maturité
                        lexifi_id = isin  # Correction : ISIN uniquement
                        key = (lexifi_forward_id, forward_str, market_date)
                        if key not in existing_rows:
                            rows_to_insert.append((lexifi_forward_id, lexifi_id, forward, market_date))
                            existing_rows.add(key)
                    except Exception:
                        continue

    # Insertion par lot
    BATCH_SIZE = 1000
    for i in range(0, len(rows_to_insert), BATCH_SIZE):
        cursor.executemany(
            f"""
            INSERT INTO {TABLE_NAME} 
            ({FIELDS['lexifi_forward_id']}, {FIELDS['lexifi_id']}, {FIELDS['lexifi_forward']}, {FIELDS['lexifi_date']}) 
            VALUES (?, ?, ?, ?)
            """,
            rows_to_insert[i:i+BATCH_SIZE]
        )
        conn.commit()

    file_end_time = time.time()
    elapsed_time = file_end_time - file_start_time
    print(f"✅ [{file_count}/{len(csv_files)}] {len(rows_to_insert)} donnée(s) ajoutée(s) depuis {filename} en {elapsed_time:.2f} secondes.\n")

cursor.close()
conn.close()

total_end_time = time.time()
total_elapsed_time = total_end_time - total_start_time
print(f"🎉 Traitement terminé en {total_elapsed_time:.2f} secondes pour {file_count} fichier(s) traité(s).")