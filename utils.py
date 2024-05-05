import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
from tqdm import tqdm
import pandas as pd
import os
from tqdm import tqdm
import polars as pl 
import hashlib
from dateutil import parser

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

def download_file(url, directory='./data/downloaded_files'):
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

    os.makedirs('./data/downloaded_files', exist_ok=True)

    total_files = len(xlsx_links)
    progress = tqdm(total=total_files, unit='files', desc="Downloading files")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(download_file, link): link for link in xlsx_links}
        for future in as_completed(futures):
            progress.update(1)
    progress.close()


def read_excel_header(file_path, skip=0):
    """Attempt to read just the header of an Excel file with varying skip rows."""
    try:
        col_list = pd.read_excel(file_path, nrows=0, skiprows=skip).columns
        return col_list
    except Exception as e:
        print(f"Error reading header from {file_path}: {e}")
        return []

def find_unique_schemas(directory):
    """Finds and returns unique schemas from Excel files in a given directory."""
    schemas = {}
    for filename in tqdm(os.listdir(directory)):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(directory, filename)
            try:
                columns = read_excel_header(file_path)
                num_columns = len(columns)
                skip_r=1
                while num_columns <3:
                    columns = read_excel_header(file_path, skip=skip_r)
                    num_columns = len(columns)
                    skip_r+=1
                schema_key = tuple(columns)  # Use tuple of columns as a unique schema key
            except Exception as e:
                print(f"Failed to open file {filename}: {e}")

            if schema_key not in schemas:
                schemas[schema_key] = []
            schemas[schema_key].append(filename)
    return schemas


# Function to create a hash of the content
def create_hash(row):
    hash_obj = hashlib.sha256()
    combined_text = ''.join(str(row[col]) for col in list(row.index))
    hash_obj.update(combined_text.encode('utf-8'))
    return hash_obj.hexdigest()


def standardize_dates(text):
    dict_months = return_months()
    # First replace month abbreviations if any
    for abbr, num in dict_months.items():
        if abbr in text:
            text = text.replace(abbr, num)
    
    # Custom handling for 'm-Y-d' format
    if '-' in text and text.count('-') == 2:
        parts = text.split('-')
        if len(parts[0]) < 4:  # This checks if the first part is likely a year
            # Assuming the format is m-Y-d (e.g., '2-2014-01')
            # Rearrange to Y-m-d
            text = f"{parts[1]}-{parts[0].zfill(2)}-{parts[2]}"

    # Try to parse the date
    try:
        date = parser.parse(text)
        return date.strftime('%Y-%m-%d')
    except ValueError:
        # If parsing fails, return the original text or handle it as needed
        return text


def return_final_schema():
    final_schema_columns = ['Fecha',  'Sigla Empresa', 'Nombre Empresa',
                        'Origen', 'Destino', 'Apto_Origen', 'Apto_Destino',
                        'Pasajeros', 'Trafico', 'TipoVuelo', 'Ciudad Origen', 
                        'Ciudad Destino', 'Pais Origen', 'Pais Destino']
    return final_schema_columns

def return_months():
    dict_months ={
    "Jan":"01","Ene":"01","Feb":"02","Mar":"03","Apr":"04","Abr":"04","May":"05", "Jun":"06",
    "Jul":"07","Aug":"08","Ago":"08","Sep":"09","Oct":"10","Nov":"11", "Dec":"12","Dic":"12"}

    return dict_months

def return_schema_mappings():
    schema_mappings = {
    # Group similar handling schemas together
    'group1': [0, 4, 5, 10],
    'group2': [1, 2, 3, 6,7, 11, 12],
    'group3': [8],
    'group4': [9],
    'group5': [13,14]}
    return schema_mappings

def return_filename_to_schema_index(directory):
    unique_schemas = find_unique_schemas(directory)
    schema_index = {filename: index for index, schemas in enumerate(unique_schemas.values()) for filename in schemas}
    return schema_index

def processing_files(directory):

    df_list = []
    schema_mappings = return_schema_mappings()
    final_schema_columns= return_final_schema()
    filename_to_schema_index = return_filename_to_schema_index(directory)
    unique_schemas = find_unique_schemas(directory)

    all_files = os.listdir(directory)
    for filename in tqdm(all_files):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(directory, filename)
            skip_r = 0
            columns = read_excel_header(file_path, skip=skip_r)
            while len(columns) < 3 and skip_r < 10:
                skip_r += 1
                columns = read_excel_header(file_path, skip=skip_r)

            schema_index = filename_to_schema_index.get(filename)
            try:
                group_key = next(key for key, indexes in schema_mappings.items() if schema_index in indexes)
                tmp = pd.read_excel(file_path, skiprows=skip_r, dtype="str")
                
                if group_key == 'group1':
                    tmp["Año"] = tmp['Fecha'].str.slice(start=0, stop=4)
                    tmp["Mes"] = tmp['Fecha'].str.slice(start=5, stop=7)
                elif group_key == 'group2':
                    tmp = tmp.rename(columns={"Nombre": 'Nombre Empresa', "Número de Mes": "Mes", "MES": "Mes",
                                            "Nombre.1": 'Apto_Origen', "Nombre.2": 'Apto_Destino',
                                            "Tráfico (N/I)": 'Trafico', "Tipo Vuelo": 'TipoVuelo',
                                            "Apto Origen": 'Apto_Origen', "Apto Destino": 'Apto_Destino',
                                            "Fecha Visual": "Fecha", "Tráfico": "Trafico", "AÑO": "Año"})
    
                elif group_key == 'group3':
                    tmp = pd.read_excel(file_path, skiprows=1, dtype="str", sheet_name="DATOS")
                    tmp = tmp.rename(columns={"Nombre": 'Nombre Empresa', "Número de Mes": "Mes", "MES": "Mes",
                                            "Nombre.1": 'Apto_Origen', "Nombre.2": 'Apto_Destino',
                                            "Tráfico (N/I)": 'Trafico', "Tipo Vuelo": 'TipoVuelo',
                                            "Apto Origen": 'Apto_Origen', "Apto Destino": 'Apto_Destino',
                                            "Fecha Visual": "Fecha", "Tráfico": "Trafico", "AÑO": "Año"})
                elif group_key == 'group4':
                    tmp["Año"] = tmp['Fecha'].str.slice(start=0, stop=4)
                    tmp["Mes"] = tmp['Fecha'].str.slice(start=5, stop=7)
                    tmp = tmp.rename(columns={"Nombre": 'Nombre Empresa', 
                                            "Nombre.1": 'Apto_Origen', "Nombre.2": 'Apto_Destino',
                                            "Tráfico (N/I)": 'Trafico', 'Tipo Vuelo Agrupado': 'TipoVuelo',
                                            })
                elif group_key == 'group5':
                    tmp = tmp.rename(columns={"Nombre": 'Nombre Empresa', "Número de Mes": "Mes", "MES": "Mes",
                                            "Nombre.1": 'Apto_Origen', "Nombre.2": 'Apto_Destino',
                                            "Tráfico (N/I)": 'Trafico', "Tipo Vuelo": 'TipoVuelo',
                                            "Apto Origen": 'Apto_Origen', "Apto Destino": 'Apto_Destino',
                                            "Fecha Visual": "Fecha", "Tráfico": "Trafico", "AÑO": "Año"})
                    tmp["Fecha"] = tmp['Año'] +"-"+ tmp['Mes'] + "-01"
                    
                
                tmp = tmp[final_schema_columns]
                tmp["filename"] = filename
                tmp['ID'] = tmp.apply(create_hash, axis=1)
                tmp.columns = [x.capitalize().strip() for x in tmp.columns]
                pl_tmp = pl.from_pandas(tmp)
                #Formats the dates
                pl_tmp = pl_tmp.with_columns(pl.col("Fecha").map_elements(standardize_dates,return_dtype=pl.Utf8).alias("Fecha"))
                df_list.append(pl_tmp)

            except Exception as e:
                print(f"Failed to process file {filename}: {e}")

    # Concatenate all dataframes
    df = pl.concat(df_list)

    df2016 = pd.read_excel(os.path.join(directory,"Origen - Destino Mes 2016.xlsx"), skiprows=4, dtype="str")
    df2016 = df2016.rename(columns={"Nombre": 'Nombre Empresa', "Número de Mes": "Mes", "MES": "Mes",
                                            "Nombre.1": 'Apto_Origen', "Nombre.2": 'Apto_Destino',
                                            "Tráfico (N/I)": 'Trafico', "Tipo Vuelo": 'TipoVuelo',
                                            "Apto Origen": 'Apto_Origen', "Apto Destino": 'Apto_Destino',
                                            "Fecha Visual": "Fecha", "Tráfico": "Trafico", "AÑO": "Año"})
    df2016 = df2016[final_schema_columns]
    df2016["filename"] = "Origen - Destino Mes 2016.xlsx"
    df2016['ID'] = df2016.apply(create_hash, axis=1)
    df2016.columns = [x.capitalize().strip() for x in df2016.columns]
    df2016 = pl.from_pandas(df2016)
    df2016 = df2016.with_columns(pl.col("Fecha").map_elements(standardize_dates,return_dtype=pl.Utf8).alias("Fecha"))


    df_feb2023 = pd.read_excel(os.path.join(directory,"Base de Datos Origen - Destino Febrero 2023.xlsx"), skiprows=5,dtype="str")
    df_feb2023 = df_feb2023.rename(columns={"Nombre": 'Nombre Empresa', "Número de Mes": "Mes", "MES": "Mes",
                                            "Nombre.1": 'Apto_Origen', "Nombre.2": 'Apto_Destino',
                                            "Tráfico (N/I)": 'Trafico', "Tipo Vuelo": 'TipoVuelo',
                                            "Apto Origen": 'Apto_Origen', "Apto Destino": 'Apto_Destino',
                                            "Fecha Visual": "Fecha", "Tráfico": "Trafico", "AÑO": "Año"})
    df_feb2023 = df_feb2023[final_schema_columns]
    df_feb2023["filename"] = "Base de Datos Origen - Destino Febrero 2023.xlsx"
    df_feb2023['ID'] = df_feb2023.apply(create_hash, axis=1)
    df_feb2023.columns = [x.capitalize().strip() for x in df_feb2023.columns]
    df_feb2023 = pl.from_pandas(df_feb2023)
    df_feb2023 = df_feb2023.with_columns(pl.col("Fecha").map_elements(standardize_dates,return_dtype=pl.Utf8).alias("Fecha"))

    df = pl.concat([df, df2016, df_feb2023])
    df= df.unique()
    df = df.drop_nulls(subset="Fecha")
    df = df.drop_nulls(subset="Sigla empresa")

    df = df.with_columns(
                pl.col("Fecha").str.strptime(pl.Date, "%Y-%m-%d").alias("Fecha"),
                pl.col("Pasajeros").cast(pl.Int64).alias("Pasajeros"),
                pl.col("Sigla empresa").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Sigla empresa"),
                pl.col("Nombre empresa").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Nombre empresa"),
                pl.col("Origen").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Origen"),
                pl.col("Destino").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Destino"),
                pl.col("Apto_origen").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Apto_origen"),
                pl.col("Apto_destino").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Apto_destino"),
                pl.col("Trafico").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Trafico"),
                pl.col("Tipovuelo").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Tipovuelo"),
                pl.col("Ciudad origen").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Ciudad origen"),
                pl.col("Ciudad destino").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Ciudad destino"),
                pl.col("Pais origen").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Pais origen"),
                pl.col("Pais destino").str.to_uppercase().str.replace_all("[^a-zA-Z0-9 ]", "").alias("Pais destino"),				
                )
    return df

def save_silver(df, dir):
    os.makedirs(dir, exist_ok=True)
    file_dir = os.path.join(dir,"vuelosClean.csv")
    df.to_pandas().to_csv(file_dir,index=False)

# Function to remove special characters
def remove_special_characters(value):
    return pl.lit(value.replace_all("[^a-zA-Z0-9]", ""))