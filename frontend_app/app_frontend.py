import streamlit as st
import requests
import pandas as pd
import time

# Configuración inicial de la página
st.set_page_config(page_title="DataOps Monitor", layout="wide")

# Inyección de CSS: Tema "Dark Corporate Slate"
st.markdown("""
    <style>
        /* Fondo corporativo oscuro (Slate) */
        .stApp {
            background-color: #0F172A;
        }
        /* Textos principales en gris claro para contraste óptimo y sin fatiga visual */
        .stApp, .stApp p, .stApp h1, .stApp h2, .stApp h3, .stApp span {
            color: #F8FAFC !important;
            font-family: 'Inter', 'Segoe UI', sans-serif;
        }
        /* Suavizar la línea separadora */
        hr {
            border-color: #334155 !important;
        }
        /* Contenedores de DataFrames con bordes sutiles */
        [data-testid="stDataFrame"] {
            border: 1px solid #1E293B;
            border-radius: 6px;
            box-shadow: 0px 4px 10px rgba(0, 0, 0, 0.4);
        }
        /* Ajuste de color para los textos descriptivos inferiores */
        div[data-testid="caption"] {
            color: #94A3B8 !important;
            font-size: 14px !important;
            margin-top: 10px;
        }
        /* Estilización de la métrica superior */
        label[data-testid="stMetricLabel"] > div {
            color: #94A3B8 !important;
            font-weight: 600;
        }
        div[data-testid="stMetricValue"] > div {
            color: #38BDF8 !important; /* Azul técnico para resaltar el número */
        }
    </style>
""", unsafe_allow_html=True)

st.title("Centro de Control DataOps - Pipeline IoT")
st.markdown("### Extracción de datos mediante API REST intermedia")

# Contenedor para refresco en tiempo real
placeholder = st.empty()

API_URL = "http://backend-api:8000/api/telemetria"

while True:
    try:
        # Consumir la API
        response = requests.get(API_URL).json()
        
        with placeholder.container():
            if response.get("success") and response.get("data"):
                data_limpia = response["data"]
                df_limpio = pd.DataFrame(data_limpia)
                
                # Fila de Métricas Generales
                st.metric(label="Flujo de Ingesta Activo (API)", value=f"{len(df_limpio)} Eventos/s")
                st.markdown("---")
                
                # Diseño de Pantalla Dividida: ANTES vs DESPUÉS
                col_antes, col_despues = st.columns(2)
                
                with col_antes:
                    st.error("ANTES: Datos Crudos de la Planta (Simulados en Sensor)")
                    df_sucio = df_limpio.copy()
                    if not df_sucio.empty:
                        # Simulando los errores de origen
                        df_sucio['rpm'] = df_sucio['rpm'].astype(str).str.replace('.', ',')
                        df_sucio['nombre_operador'] = "   " + df_sucio['id_maquina'].str.upper() + "   " 
                        if len(df_sucio) > 2:
                            df_sucio.loc[0, 'temperatura'] = -999.0
                            df_sucio.loc[2, 'temperatura'] = -999.0
                    
                    # DataFrame ANTES con configuración visual
                    st.dataframe(
                        df_sucio[['timestamp_lectura', 'id_maquina', 'rpm', 'temperatura']], 
                        use_container_width=True, 
                        height=400,
                        column_config={
                            "rpm": st.column_config.Column("RPM", width="small"),
                            "temperatura": st.column_config.NumberColumn("Temperatura", format="%f °C", width="small")
                        }
                    )
                    st.caption("Problemas detectados: Formatos incorrectos, espacios en blanco y valores atípicos.")

                with col_despues:
                    st.success("DESPUÉS: Datos Curados y Seguros (DataOps + Ley 19.628)")
                    
                    # DataFrame DESPUÉS con configuración visual
                    st.dataframe(
                        df_limpio, 
                        use_container_width=True, 
                        height=400,
                        column_config={
                            "rpm": st.column_config.NumberColumn("RPM", width="small"),
                            "temperatura": st.column_config.NumberColumn("Temperatura", format="%.2f °C", width="small"),
                            "nombre_operador": st.column_config.TextColumn("Operador (SHA-256)", width="medium") # Limita el ancho del hash
                        }
                    )
                    st.caption("Soluciones aplicadas: Tipado estandarizado, remoción de anomalías y Hashing SHA-256 para anonimización.")
            else:
                st.warning("La API respondió con éxito, pero la tabla en PostgreSQL no tiene registros actualmente.")
                st.info("Sugerencia: Verificar el estado del Consumidor Kafka y la base de datos.")
                
    except Exception as e:
        with placeholder.container():
            st.info("Conectando con la API REST en el puerto 8000... Verificando estado del servicio.")
        
    time.sleep(2)