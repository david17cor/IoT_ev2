import streamlit as st
import requests
import pandas as pd
import time

st.set_page_config(page_title="DataOps Monitor", layout="wide")

st.markdown("""
    <style>
        .stApp { background-color: #0F172A; }
        .stApp, .stApp p, .stApp h1, .stApp h2, .stApp h3, .stApp span { color: #F8FAFC !important; font-family: 'Inter', 'Segoe UI', sans-serif; }
        hr { border-color: #334155 !important; }
        [data-testid="stDataFrame"] { border: 1px solid #1E293B; border-radius: 6px; box-shadow: 0px 4px 10px rgba(0, 0, 0, 0.4); }
        div[data-testid="caption"] { color: #94A3B8 !important; font-size: 14px !important; margin-top: 10px; }
        label[data-testid="stMetricLabel"] > div { color: #94A3B8 !important; font-weight: 600; }
        div[data-testid="stMetricValue"] > div { color: #38BDF8 !important; }
        @keyframes latido {
            0% { opacity: 0.2; transform: scale(0.9); }
            50% { opacity: 1; transform: scale(1.1); }
            100% { opacity: 1; transform: scale(1); }
        }
        div[data-testid="stMetricDelta"] > div { animation: latido 1s ease-out; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

# Variables de estado para inmunidad a datos históricos
if "last_total_crudos" not in st.session_state: st.session_state.last_total_crudos = 0
if "last_total_exitosos" not in st.session_state: st.session_state.last_total_exitosos = 0
if "anomalias_sesion" not in st.session_state: st.session_state.anomalias_sesion = 0

st.title("Centro de Control DataOps - Pipeline IoT")
st.markdown("### Arquitectura Medallón (Capa Bronce vs Capa Oro)")

placeholder = st.empty()

API_GOLD = "http://backend-api:8000/api/telemetria"
API_BRONZE = "http://backend-api:8000/api/consulta-cruda"

while True:
    try:
        res_oro = requests.get(API_GOLD).json()
        res_bronce = requests.get(API_BRONZE).json()
        
        with placeholder.container():
            if res_oro.get("success") and res_bronce.get("success"):
                df_limpio = pd.DataFrame(res_oro.get("data", []))
                df_crudo = pd.DataFrame(res_bronce.get("data", []))
                
                # Totales reales de la base de datos
                total_crudos_db = res_bronce.get("total_db", 0)
                total_limpios_db = res_oro.get("total_db", 0)
                
                # Calcular velocidad (Deltas puros)
                delta_crudos = max(0, total_crudos_db - st.session_state.last_total_crudos)
                delta_limpios = max(0, total_limpios_db - st.session_state.last_total_exitosos)
                
                # Si no es la primera carga, sumar las anomalías detectadas en este micro-lote
                if st.session_state.last_total_crudos > 0:
                    delta_anomalias = max(0, delta_crudos - delta_limpios)
                    st.session_state.anomalias_sesion += delta_anomalias
                else:
                    delta_anomalias = 0
                    
                # Actualizar memoria para el próximo ciclo
                st.session_state.last_total_crudos = total_crudos_db
                st.session_state.last_total_exitosos = total_limpios_db
                
                st.metric(label="Flujo de Ingesta Activo (API)", value=f"{delta_crudos} Eventos/s")
                st.markdown("---")
                
                col_antes, col_despues = st.columns(2)
                
                # --- CAPA BRONCE ---
                with col_antes:
                    st.error("🛑 CAPA BRONCE: Base de Datos Cruda (Telemetría Directa)")
                    st.metric(
                        label="📥 Total Crudos Extraídos", 
                        value=f"{total_crudos_db} recs",
                        delta=f"+{delta_crudos} nuevos"
                    )
                    st.dataframe(
                        df_crudo[['timestamp_lectura', 'ID_Maquina', 'Revoluciones_RPM', 'Temp_C']] if not df_crudo.empty else df_crudo, 
                        use_container_width=True, 
                        height=400
                    )
                    st.caption("Conectado a 'postgres_raw' | Almacenamiento inmutable.")

                # --- CAPA ORO ---
                with col_despues:
                    st.success("🥇 CAPA ORO: Base de Datos Curada (DataOps + Ley 19.628)")
                    
                    sub_col1, sub_col2 = st.columns(2)
                    with sub_col1:
                        st.metric(
                            label="✅ Registros Seguros", 
                            value=f"{total_limpios_db} recs",
                            delta=f"+{delta_limpios} ok"
                        )
                    with sub_col2:
                        st.metric(
                            label="⚠️ Descartes Estimados", 
                            value=f"{st.session_state.anomalias_sesion} recs",
                            delta=f"+{delta_anomalias} anomalías",
                            delta_color="inverse" 
                        )
                    
                    st.dataframe(
                        df_limpio, 
                        use_container_width=True, 
                        height=400
                    )
                    st.caption("Conectado a 'postgres_db' | Tipado, limpieza y enmascaramiento SHA-256.")
            else:
                st.warning("Esperando datos en ambas bases de datos...")
                
    except Exception as e:
        with placeholder.container():
            st.info("Conectando con APIs en puerto 8000... Verificando servicio.")
        
    time.sleep(2)