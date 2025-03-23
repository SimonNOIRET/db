import os
import pandas as pd
import pyodbc
from datetime import datetime, date
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

folder = r'C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data'
db_path = r'C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data.accdb'

conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    fr'DBQ={db_path};'
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

csv_files = sorted(
    [f for f in os.listdir(folder) if f.startswith("lexifi_market_data_") and f.endswith(".csv")],
    reverse=True
)

print(f"🔍 {len(csv_files)} fichier(s) trouvé(s). Traitement en cours...\n")

print("📥 Chargement des données existantes depuis la base...")
existing_rows = set()
for row in cursor.execute("SELECT lexifi_id, lexifi_spot, lexifi_date FROM asset_spot"):
    try:
        lexifi_id = str(row[0])
        spot_value = float(row[1])
        spot_rounded = round(spot_value, 4)
        spot_str = f"{spot_rounded:.4f}"

        lexifi_date = row[2]
        if isinstance(lexifi_date, datetime):
            lexifi_date = lexifi_date.date() 

        key = (lexifi_id, spot_str, lexifi_date)
        existing_rows.add(key)
    except Exception as e:
        print(f"⚠️ Ligne ignorée (corrompue ou non décodable) : {row} → {e}")
print(f"✅ {len(existing_rows)} enregistrements déjà présents.\n")

for filename in csv_files:
    filepath = os.path.join(folder, filename)

    try:
        date_str = filename.replace("lexifi_market_data_", "").replace(".csv", "")
        lexifi_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        print(f"⚠️ Nom de fichier invalide, ignoré : {filename}")
        continue

    print(f"📄 Traitement de {filename} (date: {lexifi_date})...")

    try:
        df = pd.read_csv(filepath, header=None, on_bad_lines='skip', low_memory=False)
    except Exception as e:
        print(f"❌ Erreur de lecture de {filename} : {e}\n")
        continue

    df_spot = df[df[0] == "Asset_spot"].copy()
    if df_spot.empty:
        print("ℹ️  Aucun 'Asset_spot' trouvé, fichier ignoré.\n")
        continue

    df_spot["date"] = lexifi_date
    rows_to_insert = []

    for _, row in df_spot.iterrows():
        try:
            lexifi_id = str(row[1]).strip()
            spot_value = row[2]

            if isinstance(spot_value, bytes):
                spot_value = spot_value.decode("utf-8", errors="ignore").strip()

            lexifi_spot = round(float(spot_value), 4)
            spot_str = f"{lexifi_spot:.4f}"

            lexifi_date = row["date"]
            if isinstance(lexifi_date, datetime):
                lexifi_date = lexifi_date.date()

            key = (lexifi_id, spot_str, lexifi_date)
            if key not in existing_rows:
                rows_to_insert.append((lexifi_id, lexifi_spot, lexifi_date))
                existing_rows.add(key)
        except Exception as conv_err:
            print(f"  ⚠️ Ligne ignorée (erreur de conversion) : {conv_err}")
            continue

    for row in rows_to_insert:
        try:
            cursor.execute("""
                INSERT INTO asset_spot (lexifi_id, lexifi_spot, lexifi_date)
                VALUES (?, ?, ?)
            """, row)
        except Exception as insert_err:
            print(f"  ❌ Erreur d'insertion pour {row[0]} : {insert_err}")

    conn.commit()
    print(f"✅ {len(rows_to_insert)} ligne(s) ajoutée(s) depuis {filename}.\n")

cursor.close()
conn.close()
print("🎉 Traitement terminé pour tous les fichiers.")