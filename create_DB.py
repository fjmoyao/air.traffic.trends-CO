import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
from io import StringIO
from tqdm import tqdm

# Load environment variables from .env file
load_dotenv()  # or pass the path if not in the same directory load_dotenv('/path/to/your/.env')

db_name = os.getenv("db_name")
db_user = os.getenv("db_user")
db_password = os.getenv("db_password")
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")

# Establish a connection to the database
conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
conn.autocommit = True
cursor = conn.cursor()

delete_table_query = "DROP TABLE IF EXISTS flights;"

# SQL to create a table with the specified schema
create_table_query = '''
CREATE TABLE IF NOT EXISTS flights (
    Fecha TEXT,
    Sigla_empresa TEXT,
    Nombre_empresa TEXT,
    Origen TEXT,
    Destino TEXT,
    Apto_origen TEXT,
    Apto_destino TEXT,
    Pasajeros INT,
    Trafico TEXT,
    Tipovuelo TEXT,
    Ciudad_origen TEXT,
    Ciudad_destino TEXT,
    Pais_origen TEXT,
    Pais_destino TEXT,
    Filename TEXT,
    Id TEXT
);
'''

# Define the schema 
data_types = {
    'Fecha': 'str',
    'Sigla_empresa': 'str',
    'Nombre_empresa': 'str',
    'Origen': 'str',
    'Destino': 'str',
    'Apto_origen': 'str',
    'Apto_destino': 'str',
    'Pasajeros': 'Int64',
    'Trafico': 'str',
    'Tipovuelo': 'str',
    'Ciudad_origen': 'str',
    'Ciudad_destino': 'str',
    'Pais_origen': 'str',
    'Pais_destino': 'str',
    'Filename': 'str',
    'Id': 'str'
}

cursor.execute(delete_table_query)
cursor.execute(create_table_query)
print("Table created successfully")

# Path to your CSV file
file_path = os.path.join(os.getcwd(),"data","silver_files","vuelosClean.csv")

# Read CSV with defined data types
df = pd.read_csv(file_path, dtype=data_types)
print("CSV loaded with specified schema.")

def upload_csv_to_db(df, cursor):
    # Convert DataFrame to CSV format without headers and index
    csv_data = df.to_csv(header=False, index=False)
    f = StringIO(csv_data)
    # Use tqdm for progress visualization
    tqdm_f = tqdm([f.getvalue()], desc="Uploading data", total=df.shape[0])
    
    # Copy data into the database
    for _ in tqdm_f:
        f.seek(0)  # Reset file pointer to the beginning after getting length
        cursor.copy_from(f, 'flights', sep=',', null="")  # Adjust parameters as necessary
    print("Data uploaded successfully")

# Path to  CSV file
file_path = os.path.join(os.getcwd(),"data","silver_files","vuelosClean.csv")

# Upload the CSV to the database
upload_csv_to_db(df, cursor)

# Close the connection
cursor.close()
conn.close()