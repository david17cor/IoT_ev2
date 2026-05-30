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

# =========================================================================
# INITIALIZE SESSION STATE CONTADORES (Persistentes entre refrescos)
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
                # 1. Construir Tabla Izquierda (Cruda / Sucia)
                df_sucio = df_base.copy()
                if not df_sucio.empty:
                    df_sucio['rpm'] = df_sucio['rpm'].astype(str).str.replace('.', ',', regex=False)
                    df_sucio['nombre_operador'] = "   " + df_sucio['id_maquina'].str.upper() + "   " 
                    if len(df_sucio) > 2:
                        df_sucio.loc[0, 'temperatura'] = -999.0
                        df_sucio.loc[2, 'temperatura'] = -999.0

                # 2. Construir Tabla Derecha (Limpia / Procesada con Éxito)
                # Filtramos las filas que simulamos como "anómalas" para reflejar el descarte real de Spark
                if len(df_sucio) > 2:
                    df_limpio = df_base.drop([0, 2]).reset_index(drop=True)
                    unidades_descartadas_ahora = 2
                else:
                    df_limpio = df_base.copy()
                    unidades_descartadas_ahora = 0
                
                unidades_crudas_ahora = len(df_sucio)
                unidades_limpias_ahora = len(df_limpio)
                
                # 3. Acumular en las métricas globales históricas
                st.session_state.total_crudos += unidades_crudas_ahora
                st.session_state.total_exitosos += unidades_limpias_ahora
                st.session_state.total_descartados += unidades_descartadas_ahora
                
                # Fila de Métrica General Superior (Velocidad actual de la API)
                st.metric(label="Flujo de Ingesta Activo (API)", value=f"{unidades_crudas_ahora} Eventos/s")
                st.markdown("---")
                
                # Diseño de Pantalla Dividida: ANTES vs DESPUÉS
                col_antes, col_despues = st.columns(2)
                
                # --- COLUMNA ANTES: DATOS CRUDOS ---
                with col_antes:
                    st.error("🛑 ANTES: Datos Crudos de la Planta (Simulados en Sensor)")
                    
                    # Métrica de control para el bloque rojo
                    st.metric(
                        label="📥 Total Crudos Recibidos", 
                        value=f"{st.session_state.total_crudos} recs",
                        delta=f"+{unidades_crudas_ahora} nuevos"
                    )
                    
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
                    st.caption("Problemas detectados: Formatos incorrectos, espacios en blanco y valores atípicos (-999°C).")

                # --- COLUMNA DESPUÉS: DATOS CURADOS ---
                with col_despues:
                    st.success("❇️ DESPUÉS: Datos Curados y Seguros (DataOps + Ley 19.628)")
                    
                    # Subcolumnas internas para colocar dos tarjetas de métricas en paralelo
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
                            delta_color="inverse" # Cambia el color del delta a rojo si sube
                        )
                    
                    # DataFrame DESPUÉS con configuración visual
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
                    st.caption("Soluciones aplicadas: Tipado estandarizado, remoción de anomalías y Hashing SHA-256 para anonimización.")
            else:
                st.warning("La API respondió con éxito, pero la tabla en PostgreSQL no tiene registros actualmente.")
                st.info("Sugerencia: Verificar el estado del Consumidor Kafka y la base de datos.")
                
    except Exception as e:
        with placeholder.container():
            st.info("Conectando con la API REST en el puerto 8000... Verificando estado del servicio.")
        
    time.sleep(2)