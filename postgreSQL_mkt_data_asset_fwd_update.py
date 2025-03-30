import os
import time
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
from scipy.optimize import curve_fit
from scipy.interpolate import PchipInterpolator, interp1d
from concurrent.futures import ProcessPoolExecutor
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

FOLDER = r'C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data'
TABLE_NAME = "asset_fwd"
INTERP_METHOD = "NS"  # Options: "NS", "PCHIP", "LINEAR"

FIELDS = {
    "lexifi_forward_id": "lexifi_forward_id",
    "lexifi_id": "lexifi_id",
    "lexifi_forward": "lexifi_forward",
    "lexifi_date": "lexifi_date"
}
TARGET_MATURITIES = list(range(1, 11))
BATCH_SIZE = 500
VACUUM_EVERY = 100
MAX_FORWARD_VALUE = 1e8

def nelson_siegel(t, beta0, beta1, beta2, tau):
    t = np.asarray(t)
    t = np.where(t == 0, 1e-6, t)
    return beta0 + beta1 * (1 - np.exp(-t / tau)) / (t / tau) + beta2 * ((1 - np.exp(-t / tau)) / (t / tau) - np.exp(-t / tau))

def interpolate_forward_curve(maturities, values, target_maturities):
    maturities = np.array(maturities)
    values = np.array(values)
    target_maturities = np.array(target_maturities)
    mask = (values > 0) & (maturities > 0)
    maturities, values = maturities[mask], values[mask]

    if len(maturities) < 2:
        return []

    try:
        if INTERP_METHOD == "NS":
            if len(maturities) < 3:
                raise ValueError("Not enough points for NS")
            log_values = np.log(values)
            bounds = ([0, -np.inf, -np.inf, 0.05], [np.inf, np.inf, np.inf, 10])
            initial = [log_values[-1], log_values[0] - log_values[-1], 0, 1.0]
            params, _ = curve_fit(nelson_siegel, maturities, log_values, p0=initial, bounds=bounds, maxfev=10000)
            log_interp = nelson_siegel(target_maturities, *params)
            return [float(min(max(np.exp(v), 0.0), MAX_FORWARD_VALUE)) for v in log_interp]

        elif INTERP_METHOD == "PCHIP":
            interp = PchipInterpolator(maturities, values, extrapolate=True)
            return [float(min(max(interp(t), 0.0), MAX_FORWARD_VALUE)) for t in target_maturities]

        elif INTERP_METHOD == "LINEAR":
            interp = interp1d(maturities, values, kind='linear', fill_value='extrapolate')
            return [float(min(max(interp(t), 0.0), MAX_FORWARD_VALUE)) for t in target_maturities]

    except:
        return []

def interpolate_asset(args):
    isin, points, date = args
    if len(points) < 2:
        return []
    points.sort()
    maturities, values = zip(*points)
    interpolated = interpolate_forward_curve(maturities, values, TARGET_MATURITIES)
    return [
        (f"{isin} {t}Y", isin, round(forward, 6), date)
        for t, forward in zip(TARGET_MATURITIES, interpolated)
    ]

if __name__ == '__main__':
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="lexifi_mkt_data",
        user="postgres",
        password="0112"
    )
    conn.autocommit = False
    cursor = conn.cursor()

    cursor.execute(f"SELECT DISTINCT {FIELDS['lexifi_date']} FROM {TABLE_NAME};")
    already_in_db = {row[0] for row in cursor.fetchall()}

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

        if file_date in already_in_db:
            print(f"⏩ [{file_count}/{len(csv_files)}] Fichier déjà traité : {filename}")
            continue

        print(f"📄 [{file_count}/{len(csv_files)}] Traitement de {filename}...")

        try:
            df = pd.read_csv(filepath, header=None, on_bad_lines='skip', low_memory=False)
            df = df[df[0].isin(["Asset_spot", "Asset_forward", "Asset_forward_growth_rate"])]
        except Exception:
            print(f"❌ Erreur lecture fichier : {filename}")
            continue

        df[3] = pd.to_datetime(df[3], format="%Y-%m-%d", errors='coerce')
        df = df[df[3].notna()].copy()

        df['split'] = df[1].astype(str).str.strip().str.split()
        df['isin'] = df['split'].apply(lambda x: x[0] if len(x) == 2 else None)
        df['maturity_str'] = df['split'].apply(lambda x: x[1] if len(x) == 2 else None)
        df = df[df['isin'].notna()]
        df['maturity_date'] = pd.to_datetime(df['maturity_str'], format="%Y-%m-%d", errors='coerce')
        df = df[df['maturity_date'].notna()].copy()

        df['maturity_years'] = (df['maturity_date'] - df[3]).dt.days / 365.0
        df = df[df['maturity_years'] > 0]

        spot_prices = {
            str(row[1]).strip(): float(row[2])
            for _, row in df[df[0] == "Asset_spot"].iterrows()
            if str(row[1]).strip() and pd.notna(row[2])
        }

        rows_to_upsert = []

        for key_name in ["Asset_forward", "Asset_forward_growth_rate"]:
            df_type = df[df[0] == key_name].copy()
            df_type['value'] = df_type.apply(
                lambda row: float(row[2]) if key_name == "Asset_forward"
                else spot_prices.get(row['isin'], 0) * np.exp(float(row[2]) * row['maturity_years'])
                if spot_prices.get(row['isin'], 0) > 0 else np.nan,
                axis=1
            )
            df_type = df_type[df_type['value'].notna()]

            grouped = df_type.groupby(['isin', 3])
            args_list = [ (isin, list(zip(group['maturity_years'], group['value'])), date) for (isin, date), group in grouped ]

            with ProcessPoolExecutor(max_workers=20) as executor:
                results = executor.map(interpolate_asset, args_list)
                for rows in results:
                    if rows:
                        rows_to_upsert.extend(rows)

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
                cursor.executemany(upsert_query, rows_to_upsert[i:i + BATCH_SIZE])
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




