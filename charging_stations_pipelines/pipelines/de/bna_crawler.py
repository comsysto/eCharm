import requests as requests
from bs4 import BeautifulSoup

from charging_stations_pipelines.shared import download_file


def get_bna_data(tmp_data_path: str):
    # Base url & header
    headers = {"User-Agent": "Mozilla/5.0"}
    base_url = "https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister.html"

    r = requests.get(base_url, headers=headers)
    soup = BeautifulSoup(r.content, "html.parser")

    # Lookup for the link in the html
    download_link = soup.find("a", class_="FTxlsx")
    path_to_file = download_link.get("href")
    full_download_link = "https://www.bundesnetzagentur.de" + path_to_file

    download_file(full_download_link, tmp_data_path)

