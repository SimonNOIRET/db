import os
import requests
import zipfile
import io
import hashlib
import json
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm

BASE_URL = "https://market-data.client.lexifi.com/market_data/lsmoM122/"
REPORT_URL = urljoin(BASE_URL, "reports/lexifi_market_data_report.zip")
USERNAME = "federal_finance"
PASSWORD = "tadJz8BPGf6N"

DEST_DIR = r"C:\Users\Simon\Documents\ArkeaAM\VSCode\Database\lexifi_mkt_data"
REPORT_DEST_PATH = r"C:\Users\Simon\Documents\ArkeaAM\VSCode\Database\lexifi_mkt_data_map.csv"
CHECKSUM_FILE = os.path.join(DEST_DIR, "checksums.json")

os.makedirs(DEST_DIR, exist_ok=True)

def extract_md_file_from_zip(zip_content):
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            for file_name in zip_file.namelist():
                if file_name.endswith(".md"):
                    with zip_file.open(file_name) as md_file:
                        try:
                            content = md_file.read().decode("utf-8")
                        except UnicodeDecodeError:
                            md_file.seek(0)
                            content = md_file.read().decode("latin-1")
                        return file_name, content
    except Exception as e:
        print(f"❌ Erreur d'extraction depuis zip : {e}")
    return None, None

def extract_csv_from_report_zip(zip_content):
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            for file_name in zip_file.namelist():
                if file_name.endswith(".csv"):
                    with zip_file.open(file_name) as csv_file:
                        return csv_file.read().decode("utf-8")
    except Exception as e:
        print(f"❌ Erreur d'extraction du rapport : {e}")
    return None

def lines_checksum(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def get_filtered_zip_links(session):
    response = session.get(BASE_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a")
    pattern = r"^lexifi_market_data_\d{4}-\d{2}-\d{2}\.zip$"

    zip_links = []
    for link in links:
        href = link.get("href", "")
        filename = os.path.basename(href)
        if re.fullmatch(pattern, filename):
            full_url = urljoin(BASE_URL, href)
            zip_links.append((filename, full_url))
    return zip_links

def load_checksums():
    if os.path.exists(CHECKSUM_FILE):
        with open(CHECKSUM_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_checksums(checksums):
    with open(CHECKSUM_FILE, "w", encoding="utf-8") as f:
        json.dump(checksums, f, indent=2)

def main():
    updated_files, unchanged_files, failed_files = [], [], []
    checksums = load_checksums()

    with requests.Session() as session:
        session.auth = (USERNAME, PASSWORD)

        try:
            zip_links = get_filtered_zip_links(session)
        except Exception as e:
            print(f"❌ Impossible de récupérer la liste des fichiers : {e}")
            return

        print(f"\n🔍 {len(zip_links)} fichiers à traiter...\n")

        for zip_filename, full_url in tqdm(zip_links, desc="Téléchargement", unit="fichier"):
            md_path = os.path.join(DEST_DIR, zip_filename.replace(".zip", ".md"))

            if zip_filename in checksums and os.path.exists(md_path):
                unchanged_files.append(zip_filename)
                continue

            try:
                response = session.get(full_url)
                if response.status_code == 200:
                    md_filename, md_content = extract_md_file_from_zip(response.content)
                    if not md_content:
                        failed_files.append(zip_filename)
                        print(f"❌ {zip_filename} → fichier .md vide ou absent")
                        continue

                    new_checksum = lines_checksum(md_content)

                    if os.path.exists(md_path):
                        with open(md_path, "r", encoding="utf-8") as f:
                            existing_content = f.read()
                        existing_checksum = lines_checksum(existing_content)
                    else:
                        existing_checksum = None

                    if existing_checksum != new_checksum:
                        with open(md_path, "w", encoding="utf-8") as f:
                            f.write(md_content)
                        updated_files.append(zip_filename)
                        checksums[zip_filename] = new_checksum
                        print(f"✅ {zip_filename} → mis à jour")
                    else:
                        unchanged_files.append(zip_filename)
                        checksums[zip_filename] = new_checksum
                        print(f"↪️ {zip_filename} → inchangé")
                else:
                    failed_files.append(zip_filename)
                    print(f"❌ {zip_filename} → HTTP {response.status_code}")
            except Exception as e:
                failed_files.append(zip_filename)
                print(f"❌ {zip_filename} → erreur : {e}")

        print("\n📄 Téléchargement du rapport complémentaire...")
        try:
            report_response = session.get(REPORT_URL)
            if report_response.status_code == 200:
                csv_content = extract_csv_from_report_zip(report_response.content)
                if csv_content:
                    with open(REPORT_DEST_PATH, "w", encoding="utf-8", newline='') as f:
                        f.write(csv_content)
                    print(f"✅ Rapport extrait et enregistré → {REPORT_DEST_PATH}")
                else:
                    print("❌ Rapport → .csv introuvable ou vide")
            else:
                print(f"❌ Rapport → erreur HTTP {report_response.status_code}")
        except Exception as e:
            print(f"❌ Rapport → erreur de récupération : {e}")

    save_checksums(checksums)

    print("\n📊 Résumé :")
    print(f"✅ Fichiers mis à jour : {len(updated_files)}")
    print(f"↪️ Fichiers inchangés : {len(unchanged_files)}")
    print(f"❌ Fichiers échoués : {len(failed_files)}")

    if updated_files:
        print("\n✅ Fichiers mis à jour :")
        for f in updated_files:
            print(f"  - {f}")

    if failed_files:
        print("\n❌ Fichiers échoués :")
        for f in failed_files:
            print(f"  - {f}")

if __name__ == "__main__":
    main()