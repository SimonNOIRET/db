import os
import pandas as pd
import pyodbc
from datetime import datetime
from scipy.interpolate import PchipInterpolator
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

# Dossier et base
folder = r'C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data'
db_path = r'C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data.accdb'
target_maturities = list(range(1, 11))  # 1Y à 10Y

# Connexion Access
conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    fr'DBQ={db_path};'
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Fichiers CSV triés
csv_files = sorted(
    [f for f in os.listdir(folder) if f.startswith("lexifi_market_data_") and f.endswith(".csv")],
    reverse=True
)

print(f"🔍 {len(csv_files)} fichier(s) trouvé(s). Traitement en cours pour Asset_forward...\n")

# Charger les données existantes
existing_rows = set()
for row in cursor.execute("SELECT lexifi_id, lexifi_forward, lexifi_date FROM asset_fwd"):
    try:
        lexifi_id = str(row[0])
        forward = round(float(row[1]), 4)
        forward_str = f"{forward:.4f}"
        lexifi_date = row[2]
        if isinstance(lexifi_date, datetime):
            lexifi_date = lexifi_date.date()
        existing_rows.add((lexifi_id, forward_str, lexifi_date))
    except Exception:
        continue

# Traitement fichier par fichier
for filename in csv_files:
    filepath = os.path.join(folder, filename)

    try:
        date_str = filename.replace("lexifi_market_data_", "").replace(".csv", "")
        file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        print(f"⚠️ Fichier ignoré (nom invalide) : {filename}")
        continue

    print(f"📄 Traitement de {filename} (date marché : {file_date})...")

    try:
        df = pd.read_csv(filepath, header=None, on_bad_lines='skip', low_memory=False)
    except Exception:
        print(f"❌ Erreur lecture fichier : {filename}")
        continue

    df_forward = df[df[0] == "Asset_forward"].copy()
    if df_forward.empty:
        print("ℹ️ Aucun 'Asset_forward' trouvé.\n")
        continue

    df_forward["market_date"] = pd.to_datetime(df_forward[3], errors='coerce').dt.date
    df_forward = df_forward[df_forward["market_date"].notna()].copy()

    rows_to_insert = []

    for market_date, group in df_forward.groupby("market_date"):
        forward_curves = {}

        for _, row in group.iterrows():
            try:
                parts = str(row[1]).strip().split()
                if len(parts) != 2:
                    continue
                isin, maturity_str = parts
                forward_value = float(row[2])
                maturity_date = datetime.strptime(maturity_str, "%Y-%m-%d").date()
                maturity_years = (maturity_date - market_date).days / 365.0
                if maturity_years <= 0:
                    continue
                forward_curves.setdefault(isin, []).append((maturity_years, forward_value))
            except Exception:
                continue

        for isin, points in forward_curves.items():
            if len(points) < 2:
                continue

            points.sort()
            maturities, values = zip(*points)

            try:
                interpolator = PchipInterpolator(maturities, values, extrapolate=True)
            except Exception:
                continue

            for t in target_maturities:
                try:
                    forward = float(interpolator(t))
                    forward = max(forward, 0.0)  # Sécurité si tu veux forcer positifs
                    forward = round(forward, 4)
                    forward_str = f"{forward:.4f}"
                    lexifi_id = f"{isin} {t}Y"
                    key = (lexifi_id, forward_str, market_date)
                    if key not in existing_rows:
                        rows_to_insert.append((lexifi_id, forward, market_date))
                        existing_rows.add(key)
                except Exception:
                    continue

    for row in rows_to_insert:
        try:
            cursor.execute("""
                INSERT INTO asset_fwd (lexifi_id, lexifi_forward, lexifi_date)
                VALUES (?, ?, ?)
            """, row)
        except Exception:
            continue

    conn.commit()
    print(f"✅ {len(rows_to_insert)} donnée(s) ajoutée(s) depuis {filename}.\n")

cursor.close()
conn.close()
print("🎉 Traitement terminé pour Asset_forward.")