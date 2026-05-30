import streamlit as st
import requests
import pandas as pd
import time
import random

# Configuración inicial de la página
st.set_page_config(page_title="DataOps Monitor", layout="wide")

# Inyección de CSS: Tema "Dark Corporate Slate" + Animación de Parpadeo
st.markdown("""
    <style>
        /* Fondo corporativo oscuro (Slate) */
        .stApp {
            background-color: #0F172A;
        }
        /* Textos principales en gris claro */
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
            color: #38BDF8 !important;
        }
        
        /* 🔴 ANIMACIÓN DE PARPADEO PARA LOS DELTAS (+20, +18, etc.) 🔴 */
        @keyframes latido {
            0% { opacity: 0.2; transform: scale(0.9); }
            50% { opacity: 1; transform: scale(1.1); }
            100% { opacity: 1; transform: scale(1); }
        }
        /* Aplicamos la animación al componente delta de Streamlit */
        div[data-testid="stMetricDelta"] > div {
            animation: latido 1s ease-out;
            font-weight: bold;
        }
    </style>
""", unsafe_allow_html=True)

# =========================================================================
# INITIALIZE SESSION STATE CONTADORES
# =========================================================================
if "total_crudos" not in st.session_state:
    st.session_state.total_crudos = 0
if "total_exitosos" not in st.session_state:
    st.session_state.total_exitosos = 0
if "total_descartados" not in st.session_state:
    st.session_state.total_descartados = 0

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
                data_api = response["data"]
                df_base = pd.DataFrame(data_api)
                
                # --- LÓGICA DE SIMULACIÓN ANTES VS DESPUÉS ---
                df_sucio = df_base.copy()
                total_filas = len(df_sucio)
                unidades_descartadas_ahora = 0
                indices_anomalos = []
                
                if not df_sucio.empty:
                    # Problemas estéticos
                    df_sucio['rpm'] = df_sucio['rpm'].astype(str).str.replace('.', ',', regex=False)
                    df_sucio['nombre_operador'] = "   " + df_sucio['id_maquina'].str.upper() + "   " 
                    
                    # 🎲 Lógica de Probabilidad Dinámica (20% a 30% de anomalías)
                    if total_filas > 0:
                        porcentaje_falla = random.uniform(0.20, 0.30)
                        num_anomalies = int(total_filas * porcentaje_falla)
                        
                        # Elegimos filas al azar para inyectar el veneno
                        if num_anomalies > 0:
                            indices_anomalos = random.sample(range(total_filas), num_anomalies)
                            for idx in indices_anomalos:
                                df_sucio.loc[idx, 'temperatura'] = -999.0

                # 2. Construir Tabla Derecha (Limpia / Procesada con Éxito)
                if len(indices_anomalos) > 0:
                    df_limpio = df_base.drop(indices_anomalos).reset_index(drop=True)
                    unidades_descartadas_ahora = len(indices_anomalos)
                else:
                    df_limpio = df_base.copy()
                    unidades_descartadas_ahora = 0
                
                unidades_crudas_ahora = len(df_sucio)
                unidades_limpias_ahora = len(df_limpio)
                
                # 3. Acumular en las métricas globales
                st.session_state.total_crudos += unidades_crudas_ahora
                st.session_state.total_exitosos += unidades_limpias_ahora
                st.session_state.total_descartados += unidades_descartadas_ahora
                
                # Fila de Métrica General Superior
                st.metric(label="Flujo de Ingesta Activo (API)", value=f"{unidades_crudas_ahora} Eventos/s")
                st.markdown("---")
                
                # Diseño de Pantalla Dividida
                col_antes, col_despues = st.columns(2)
                
                # --- COLUMNA ANTES: DATOS CRUDOS ---
                with col_antes:
                    st.error("🛑 ANTES: Datos Crudos de la Planta (Simulados en Sensor)")
                    st.metric(
                        label="📥 Total Crudos Recibidos", 
                        value=f"{st.session_state.total_crudos} recs",
                        delta=f"+{unidades_crudas_ahora} nuevos"
                    )
                    st.dataframe(
                        df_sucio[['timestamp_lectura', 'id_maquina', 'rpm', 'temperatura']], 
                        use_container_width=True, 
                        height=400,
                        column_config={
                            "rpm": st.column_config.Column("RPM", width="small"),
                            "temperatura": st.column_config.NumberColumn("Temperatura", format="%f °C", width="small")
                        }
                    )
                    st.caption("Problemas detectados: Formatos incorrectos y valores atípicos (-999°C).")

                # --- COLUMNA DESPUÉS: DATOS CURADOS ---
                with col_despues:
                    st.success("❇️ DESPUÉS: Datos Curados y Seguros (DataOps + Ley 19.628)")
                    
                    sub_col1, sub_col2 = st.columns(2)
                    with sub_col1:
                        st.metric(
                            label="✅ Procesados con Éxito", 
                            value=f"{st.session_state.total_exitosos} recs",
                            delta=f"+{unidades_limpias_ahora} ok"
                        )
                    with sub_col2:
                        st.metric(
                            label="⚠️ Total Descartados", 
                            value=f"{st.session_state.total_descartados} recs",
                            delta=f"+{unidades_descartadas_ahora} anomalías",
                            delta_color="inverse" 
                        )
                    
                    st.dataframe(
                        df_limpio, 
                        use_container_width=True, 
                        height=400,
                        column_config={
                            "rpm": st.column_config.NumberColumn("RPM", width="small"),
                            "temperatura": st.column_config.NumberColumn("Temperatura", format="%.2f °C", width="small"),
                            "nombre_operador": st.column_config.TextColumn("Operador (SHA-256)", width="medium")
                        }
                    )
                    st.caption("Soluciones: Tipado estandarizado, remoción de anomalías y Hashing SHA-256.")
            else:
                st.warning("La API respondió con éxito, pero la tabla en PostgreSQL no tiene registros actualmente.")
                
    except Exception as e:
        with placeholder.container():
            st.info("Conectando con la API REST en el puerto 8000... Verificando estado del servicio.")
        
    time.sleep(2)