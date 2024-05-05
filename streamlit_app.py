import streamlit as st
import pandas as pd
import polars as pl
import plotly.express as px
import os 
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError
load_dotenv()

data_file = os.path.join(os.getcwd(),"data","silver_files")
db_name = os.getenv("db_name")
db_user = os.getenv("db_user")
db_password = os.getenv("db_password")
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")

# Establish a connection to the database
conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
conn.autocommit = True
cursor = conn.cursor()

# Load your data (assuming a CSV file for simplicity)
#@st.cache
def fetch_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        data = cursor.fetchall()  # Fetch all results
        columns = [desc[0] for desc in cursor.description]  # This gets the column names
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        print(f"The error '{e}' occurred")
        return None
select_query = "SELECT * FROM flights LIMIT 100;"

df = fetch_query(conn, select_query)
df["fecha"] = pd.to_datetime(df["fecha"])
# Convert the DataFrame from pandas to polars for specific manipulations if needed
df_pl = pl.from_pandas(df)

# Sidebar for user inputs or filters
st.sidebar.header('Filter Options')
year = st.sidebar.selectbox('Select Year', df['fecha'].dt.year.unique())

# Main page layout
st.title('Flight Data Dashboard')
st.write(df_pl.select(pl.col('fecha')))
# Visualization 1: Flight Traffic Analysis by Destination
st.header('Flight Traffic Analysis by Destination')
destination_traffic = df_pl.filter(pl.col('fecha').dt.year == year).groupby('destino').agg(
    pl.count().alias('Flight Count')
).sort('Flight Count', reverse=True).to_pandas()
fig1 = px.bar(destination_traffic, x='destino', y='Flight Count', title='Flight Traffic by Destination')
st.plotly_chart(fig1)

# Visualization 2: Passenger Volume Trends Over Time
st.header('Passenger Volume Trends Over Time')
passenger_trends = df.groupby(df['fecha'].dt.to_period("M")).agg({'pasajeros': 'sum'}).reset_index()
fig2 = px.line(passenger_trends, x='fecha', y='pasajeros', title='Monthly Passenger Volume Trends')
st.plotly_chart(fig2)

# Visualization 3: Comparison of Flight Volume by Type of Flight
st.header('Comparison of Flight Volume by Type of Flight')
flight_type_comparison = df_pl.groupby('tipovuelo').agg(
    pl.count().alias('Flight Count')
).to_pandas()
fig3 = px.pie(flight_type_comparison, names='tipovuelo', values='Flight Count', title='Flight Volume by Type')
st.plotly_chart(fig3)

# Ensure to add installation instructions and setup for required libraries and Streamlit in your documentation.
