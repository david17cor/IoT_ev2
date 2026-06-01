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

if "total_crudos" not in st.session_state: st.session_state.total_crudos = 0
if "total_exitosos" not in st.session_state: st.session_state.total_exitosos = 0
if "total_descartados" not in st.session_state: st.session_state.total_descartados = 0

st.title("Centro de Control DataOps - Pipeline IoT")
st.markdown("### Arquitectura Medallón (Capa Bronce vs Capa Oro)")

placeholder = st.empty()

API_GOLD = "http://backend-api:8000/api/telemetria"
API_BRONZE = "http://backend-api:8000/api/consulta-cruda"

while True:
    try:
        # Consumir ambas APIs reales
        res_oro = requests.get(API_GOLD).json()
        res_bronce = requests.get(API_BRONZE).json()
        
        with placeholder.container():
            if res_oro.get("success") and res_bronce.get("success"):
                df_limpio = pd.DataFrame(res_oro.get("data", []))
                df_crudo = pd.DataFrame(res_bronce.get("data", []))
                
                unidades_crudas_ahora = len(df_crudo)
                unidades_limpias_ahora = len(df_limpio)
                # Estimación de descartes en el micro-lote visual
                unidades_descartadas_ahora = max(0, unidades_crudas_ahora - unidades_limpias_ahora) 
                
                st.session_state.total_crudos += unidades_crudas_ahora
                st.session_state.total_exitosos += unidades_limpias_ahora
                st.session_state.total_descartados += unidades_descartadas_ahora
                
                st.metric(label="Flujo de Ingesta Activo (API)", value=f"{unidades_crudas_ahora} Eventos/s")
                st.markdown("---")
                
                col_antes, col_despues = st.columns(2)
                
                # --- CAPA BRONCE ---
                with col_antes:
                    st.error("🛑 CAPA BRONCE: Base de Datos Cruda (Telemetría Directa)")
                    st.metric(
                        label="📥 Total Crudos Extraídos", 
                        value=f"{st.session_state.total_crudos} recs",
                        delta=f"+{unidades_crudas_ahora} nuevos"
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
                            value=f"{st.session_state.total_exitosos} recs",
                            delta=f"+{unidades_limpias_ahora} ok"
                        )
                    with sub_col2:
                        st.metric(
                            label="⚠️ Descartes Estimados", 
                            value=f"{st.session_state.total_descartados} recs",
                            delta=f"+{unidades_descartadas_ahora} anomalías",
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