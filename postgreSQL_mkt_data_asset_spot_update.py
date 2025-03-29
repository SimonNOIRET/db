import os
import pandas as pd
import psycopg2
from datetime import datetime
import warnings
import time

warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

folder = r'C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data'

def get_pg_connection():
    return psycopg2.connect(
        dbname="lexifi_mkt_data",
        user="postgres",
        password="0112",
        host="localhost",
        port="5432"
    )

table_name = "asset_spot"
fields = {
    "id": "lexifi_id",
    "spot": "lexifi_spot",
    "date": "lexifi_date"
}
BATCH_SIZE = 500

conn = get_pg_connection()
cursor = conn.cursor()

csv_files = sorted(
    [f for f in os.listdir(folder) if f.startswith("lexifi_market_data_") and f.endswith(".csv")],
    reverse=True
)

total_files = len(csv_files)
print(f"\n🔍 {total_files} fichier(s) trouvé(s). Traitement en cours...\n")
start_global = time.time()

print("📥 Chargement des données existantes depuis la base...")

query = f"SELECT {fields['id']}, {fields['spot']}, {fields['date']} FROM {table_name}"
existing_df = pd.read_sql(query, conn)

existing_df[fields['id']] = existing_df[fields['id']].astype(str).str.strip()
existing_df[fields['spot']] = existing_df[fields['spot']].astype(float).round(4)
existing_df[fields['date']] = pd.to_datetime(existing_df[fields['date']]).dt.date

existing_rows = set(existing_df.itertuples(index=False, name=None))
print(f"✅ {len(existing_rows)} enregistrements déjà présents.\n")
print("=" * 60)

processed_files = 0

for filename in csv_files:
    file_start = time.time()
    processed_files += 1

    filepath = os.path.join(folder, filename)

    try:
        date_str = filename.replace("lexifi_market_data_", "").replace(".csv", "")
        lexifi_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        print(f"⚠️ Nom de fichier invalide, ignoré : {filename}")
        continue

    print(f"📄 [{processed_files}/{total_files}] Traitement de `{filename}` (Date: {lexifi_date})...")

    try:
        df = pd.read_csv(filepath, header=None, on_bad_lines='skip', dtype={1: str, 2: str})
    except Exception as e:
        print(f"❌ Erreur de lecture de `{filename}` : {e}\n")
        continue

    df_spot = df[df[0] == "Asset_spot"].copy()
    if df_spot.empty:
        print("ℹ️ Aucun 'Asset_spot' trouvé, fichier ignoré.\n")
        continue

    df_spot.rename(columns={1: fields['id'], 2: fields['spot']}, inplace=True)
    df_spot[fields['spot']] = pd.to_numeric(df_spot[fields['spot']], errors='coerce').round(4)
    df_spot[fields['date']] = lexifi_date

    df_spot[fields['id']] = df_spot[fields['id']].astype(str).str.strip()
    new_rows = [
        (row[fields['id']], row[fields['spot']], row[fields['date']])
        for _, row in df_spot.iterrows()
        if (row[fields['id']], row[fields['spot']], row[fields['date']]) not in existing_rows
    ]

    if not new_rows:
        print(f"✅ Aucune nouvelle donnée à insérer pour `{filename}`.\n")
        continue

    try:
        for i in range(0, len(new_rows), BATCH_SIZE):
            batch = new_rows[i:i + BATCH_SIZE]
            insert_query = f"""
                INSERT INTO {table_name} ({fields['id']}, {fields['spot']}, {fields['date']})
                VALUES (%s, %s, %s)
            """
            cursor.executemany(insert_query, batch)
            conn.commit()
        print(f"✅ {len(new_rows)} ligne(s) ajoutée(s) depuis `{filename}`.")

        existing_rows.update(new_rows)

    except Exception as insert_err:
        print(f"❌ Erreur d'insertion pour `{filename}` : {insert_err}")
        conn.rollback()

    file_end = time.time()
    print(f"⏱️ Temps de traitement : {file_end - file_start:.2f} secondes.\n")
    print("=" * 60)

cursor.close()
conn.close()

end_global = time.time()
print(f"🎉 Traitement terminé pour tous les fichiers en {end_global - start_global:.2f} secondes.")
print(f"📊 {processed_files}/{total_files} fichiers traités.")