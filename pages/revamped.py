import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DB_CONFIG = {
    "dbname": os.getenv("db_name"),
    "user": os.getenv("db_user"),
    "password": os.getenv("db_password"),
    "host": os.getenv("db_host"),
    "port": os.getenv("db_port")
}

def get_connection():
    """Establishes a database connection using the configuration provided."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as e:
        st.error("Database connection failed: " + str(e))
        return None

@st.cache_data(ttl=600, show_spinner=False)
def run_query(query):
    """Executes a given SQL query and returns a pandas DataFrame."""
    with get_connection() as conn:
        if conn is not None:
            try:
                return pd.read_sql_query(query, conn)
            except Exception as e:
                st.error("Failed to execute query: " + str(e))
                return pd.DataFrame()


def setup_sidebar(df_years):
    """Set up sidebar components and return selected options."""
    year = st.sidebar.selectbox("Año", df_years['year'])
    return year

def create_plots(df_airlines, df_types, df_traffic):
    """Create and display plots for the dashboard."""
    if df_airlines.empty or df_types.empty or df_traffic.empty:
        st.warning("No data available for the selected year.")
        return

    # Airline Passengers Volume
    fig_airlines = px.bar(df_airlines, y='nombre_empresa', x='volumen', text='pct',
                          labels={'volumen': 'Cantidad de pasajeros'},
                          title='Volumen de pasajeros por Aerolínea')
    fig_airlines.update_traces(texttemplate='%{x:.3s}<br>%{text}', textposition='outside')
    fig_airlines.update_layout(uniformtext_minsize=8, uniformtext_mode='hide', yaxis_tickformat=',')
    
    # Passenger Volume by Flight Type
    fig_types = px.bar(df_types, x='tipovuelo', y='volumen', text='pct',
                       labels={'volumen': 'Cantidad de pasajeros'},
                       title='Volumen de pasajeros por tipo de vuelo')
    fig_types.update_traces(texttemplate='%{y:.3s}<br>%{text}', textposition='outside')
    fig_types.update_layout(uniformtext_minsize=8, uniformtext_mode='hide', yaxis_tickformat=',')

    # Traffic Type Volume
    fig_traffic = px.pie(df_traffic, names='trafico', values='volumen',
                         title='Volumen de pasajeros por tipo de tráfico')
    fig_traffic.update_traces(textinfo='label+percent', hoverinfo='label+value+percent')
    fig_traffic.update_layout(legend_title_text='Tipo de Tráfico aéreo')
    
    # Display plots
    st.plotly_chart(fig_airlines)
    col1, col2 = st.columns(2)
    col1.plotly_chart(fig_types)
    col2.plotly_chart(fig_traffic)


def main():
    st.header("Dashboard Tráfico Aéreo Colombia")
    df_years = run_query("SELECT DISTINCT EXTRACT(YEAR FROM fecha) as year FROM flights ORDER BY year DESC;")
    year = setup_sidebar(df_years)

    query_base = f"WHERE EXTRACT(YEAR FROM fecha) = {year}"
    df_airlines = run_query(f"SELECT nombre_empresa, SUM(pasajeros) as volumen, ( ( CAST(SUM(pasajeros) AS DECIMAL)/(SELECT COUNT(*) FROM flights) ) ) as pct FROM flights {query_base} GROUP BY nombre_empresa ORDER BY volumen DESC;")
    df_types = run_query(f"SELECT tipovuelo, SUM(pasajeros) as volumen, ( ( CAST(SUM(pasajeros) AS DECIMAL)/(SELECT COUNT(*) FROM flights) ) ) as pct FROM flights {query_base} GROUP BY tipovuelo ORDER BY volumen DESC;")
    df_traffic = run_query(f"SELECT trafico, SUM(pasajeros) as volumen, ( ( CAST(SUM(pasajeros) AS DECIMAL)/(SELECT COUNT(*) FROM flights) ) ) as pct FROM flights {query_base} GROUP BY trafico ORDER BY volumen DESC;")
    st.write(df_airlines)
    create_plots(df_airlines, df_types, df_traffic)

main()
