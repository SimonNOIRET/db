import os
import requests
import zipfile
import io
import hashlib
import csv
import json
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm

URL = "https://market-data.client.lexifi.com/market_data/lsmoM122/"
USERNAME = "federal_finance"
PASSWORD = "tadJz8BPGf6N"
DEST_DIR = r"C:\Users\Simon\Documents\ArkeaAM\VSCode\lexifi_mkt_data"

CHECKSUM_FILE = os.path.join(DEST_DIR, "checksums.json")
os.makedirs(DEST_DIR, exist_ok=True)

def extract_md_lines_from_zip(zip_content):
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            for file_name in zip_file.namelist():
                if file_name.endswith(".md"):
                    with zip_file.open(file_name) as md_file:
                        try:
                            lines = [line.decode("utf-8").strip() for line in md_file.readlines()]
                        except UnicodeDecodeError:
                            md_file.seek(0)
                            lines = [line.decode("latin-1").strip() for line in md_file.readlines()]
                        return lines
    except Exception as e:
        print(f"❌ Erreur d'extraction depuis zip : {e}")
    return []

def lines_checksum(lines):
    joined = "\n".join(lines).encode("utf-8")
    return hashlib.md5(joined).hexdigest()

def get_remote_file_list(session):
    response = session.get(URL)
    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a")

    zip_links = []
    for link in links:
        href = link.get("href", "")
        if href.endswith(".zip"):
            zip_filename = os.path.basename(href)

            if re.fullmatch(r"lexifi_market_data\.zip", zip_filename):
                continue

            full_url = urljoin(URL, href)
            zip_links.append((zip_filename, full_url))
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
    updated_files = []
    unchanged_files = []
    failed_files = []

    checksums = load_checksums()

    with requests.Session() as session:
        session.auth = (USERNAME, PASSWORD)
        try:
            zip_links = get_remote_file_list(session)
        except Exception as e:
            print(f"❌ Impossible de récupérer la liste des fichiers : {e}")
            return

        print(f"\n🔍 {len(zip_links)} fichiers trouvés. Traitement en cours...\n")

        for zip_filename, full_url in tqdm(zip_links, desc="Téléchargement", unit="fichier"):
            csv_path = os.path.join(DEST_DIR, zip_filename.replace(".zip", ".csv"))

            if zip_filename in checksums and os.path.exists(csv_path):
                unchanged_files.append(zip_filename)
                continue

            try:
                response = session.get(full_url)
                if response.status_code == 200:
                    md_lines = extract_md_lines_from_zip(response.content)
                    if not md_lines:
                        failed_files.append(zip_filename)
                        print(f"❌ {zip_filename} → fichier .md vide ou absent")
                        continue

                    csv_lines = [line for line in md_lines if ";" in line]
                    if len(csv_lines) < 2:
                        print(f"⚠️ {zip_filename} → peu de lignes exploitables ({len(csv_lines)})")

                    new_checksum = lines_checksum(md_lines)

                    if os.path.exists(csv_path):
                        with open(csv_path, "r", encoding="utf-8") as f:
                            existing_lines = [line.strip() for line in f.readlines()]
                        existing_checksum = lines_checksum(existing_lines)
                    else:
                        existing_checksum = None

                    if existing_checksum != new_checksum:
                        with open(csv_path, "w", newline='', encoding='utf-8') as f:
                            writer = csv.writer(f)
                            for line in csv_lines:
                                writer.writerow(line.split(";"))
                        updated_files.append(zip_filename)
                        checksums[zip_filename] = new_checksum
                        print(f"✅ {zip_filename} → mis à jour et converti")
                    else:
                        unchanged_files.append(zip_filename)
                        checksums[zip_filename] = new_checksum
                        print(f"↪️ {zip_filename} → inchangé")
                else:
                    failed_files.append(zip_filename)
                    print(f"❌ {zip_filename} → erreur HTTP {response.status_code}")
            except Exception as e:
                failed_files.append(zip_filename)
                print(f"❌ {zip_filename} → exception : {e}")

    save_checksums(checksums)

    print("\n📊 Résumé :")
    print(f"✅ Fichiers mis à jour : {len(updated_files)}")
    print(f"↪️ Fichiers inchangés : {len(unchanged_files)}")
    print(f"❌ Fichiers échoués : {len(failed_files)}")

    if updated_files:
        print("\n✅ Détails des fichiers mis à jour :")
        for f in updated_files:
            print(f"  - {f}")

    if failed_files:
        print("\n❌ Détails des erreurs :")
        for f in failed_files:
            print(f"  - {f}")

if __name__ == "__main__":
    main()