import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
from tqdm import tqdm

def fetch_page(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def extract_xlsx_links(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('.xlsx') and 'Destino' in href:
            full_url = href if href.startswith('http') else f"{base_url}/{href}"
            links.append(full_url)
    return links

def prune_files(links, year=2014):
    pattern = r"(\d{4})"
    new_links = [link for link in links if int(re.search(pattern, link).group(0)) >= year]
    return new_links

def download_file(url, directory="./downloaded_files"):
    local_filename = url.split('/')[-1]
    file_path = os.path.join(directory, local_filename)
    if not os.path.exists(file_path):
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)
    return local_filename

def download_save_files():
    url = 'https://www.aerocivil.gov.co/atencion/estadisticas-de-las-actividades-aeronauticas/bases-de-datos'
    html_content = fetch_page(url)
    xlsx_links = extract_xlsx_links(html_content, url)
    xlsx_links = prune_files(xlsx_links)

    os.makedirs('./downloaded_files', exist_ok=True)

    total_files = len(xlsx_links)
    progress = tqdm(total=total_files, unit='files', desc="Downloading files")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(download_file, link): link for link in xlsx_links}
        for future in as_completed(futures):
            progress.update(1)
    progress.close()

if __name__ == "__main__":
    download_save_files()
