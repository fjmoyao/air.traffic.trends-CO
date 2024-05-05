import streamlit as st
import pandas as pd
import polars as pl
import plotly.express as px
import os 
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError
from millify import millify
import numpy as np
import plotly.express as px

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


#Defines queries 
#select_query = "SELECT * FROM flights LIMIT 100;"
#df = fetch_query(conn, select_query)

query_anhos = fetch_query(conn,"SELECT DISTINCT EXTRACT(YEAR FROM fecha) as year FROM flights ORDER BY year DESC;" )


### App Setup
st.header("Dashboard Trafico Aereo Colombia")
year = st.sidebar.selectbox(label="año", options=query_anhos)

query_aerolineas = fetch_query(conn,f"SELECT DISTINCT nombre_empresa FROM flights WHERE EXTRACT(YEAR FROM fecha) = '{year}' ORDER BY nombre_empresa;" )
query_total_passengers = fetch_query(conn, f"SELECT SUM(pasajeros) as total_passengers FROM flights  WHERE EXTRACT(YEAR FROM fecha) = '{year}';")
query_top_aerolineas =  fetch_query(conn,f"SELECT nombre_empresa, SUM(pasajeros) as volumen FROM flights WHERE EXTRACT(YEAR FROM fecha)= '{year}' GROUP BY nombre_empresa ORDER BY volumen DESC;" )
query_tipo_vuelo = fetch_query(conn,f"SELECT tipovuelo, SUM(pasajeros) as volumen FROM flights WHERE EXTRACT(YEAR FROM fecha)= '{year}' GROUP BY tipovuelo ORDER BY volumen DESC;")
query_trafico = fetch_query(conn,f"SELECT trafico, SUM(pasajeros) as volumen FROM flights WHERE EXTRACT(YEAR FROM fecha)= '{year}' GROUP BY trafico ORDER BY volumen DESC;")

aerolineas_mercado = len(query_aerolineas)
total_pasajeros = query_total_passengers.values
top10_aerolineas = query_top_aerolineas.head(10).sort_values("volumen")
top10_aerolineas["pct"] = np.round((100*(top10_aerolineas["volumen"] /int(total_pasajeros))),1).astype("str") +"%"
tipo_vuelo = query_tipo_vuelo
tipo_vuelo["tipovuelo"] = tipo_vuelo["tipovuelo"].replace({"R":"Regular", "A":"Vuelos Adicionales","C":"Chárter","T":"Taxi Aéreo"})
tipo_vuelo["pct_volumen"] = np.round(100*(tipo_vuelo["volumen"] / tipo_vuelo["volumen"].sum()),2)
tipo_vuelo["pct_volumen"] = tipo_vuelo["pct_volumen"].astype("str") + "%"
trafico= query_trafico
trafico["trafico"] = trafico["trafico"].replace({"N":"Nacional","I":"Internacional","E":"Externo"})
trafico["pct"] = np.round((100*(trafico["volumen"] /int(total_pasajeros))),1).astype("str") +"%"
#aerolinea = st.sidebar.multiselect(label="aerolinea", options=query_aerolineas)

#filtrar = st.sidebar.button(label="Filtrar", type="primary")
c1,c2 = st.columns(2)
with c1:
    st.metric(label="Total aerolineas en el mercado", value=aerolineas_mercado)
with c2:
    st.metric(label="Total pasajeros movilizados", value=millify(total_pasajeros, precision=3))

#Figures
fig_top10 = px.bar(top10_aerolineas, y='nombre_empresa', x='volumen', text='pct',
             labels={'volumen': 'Cantidad pasajeros'},
             title='Volumen de pasajeros por Aerolínea')
# Customizing text on the bars to include pct_volumen
fig_top10.update_traces(texttemplate='%{x:.3s}<br>%{text}', textposition='outside')
fig_top10.update_layout(uniformtext_minsize=8, uniformtext_mode='hide', yaxis_tickformat=',')
st.plotly_chart(fig_top10)

col1, col2 = st.columns(2)
with col1:
    fig = px.bar(tipo_vuelo, x='tipovuelo', y='volumen', text='pct_volumen',
                labels={'volumen': 'Cantidad pasajeros'},
                title='Volumen de pasajeros por tipo de vuelo')
    # Customizing text on the bars to include pct_volumen
    fig.update_traces(texttemplate='%{y:.3s}<br>%{text}', textposition='outside')
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide', yaxis_tickformat=',')
    st.plotly_chart(fig)

with col2:
    fig_trafico = px.pie(trafico, names='trafico', values='volumen',
                title='Volumen de pasajeros por tipo de tráfico')
    # Customizing text on the bars to include pct_volumen
    fig_trafico.update_traces(textinfo ="label+percent", hoverinfo='label+value+percent')
    fig_trafico.update_layout(legend_title_text='Tipo de Tráfico aéreo')
    st.plotly_chart(fig_trafico)

