import streamlit as st
import requests
import pandas as pd
import time

# Configuración de página estricta
st.set_page_config(page_title="Dashboard Predictivo CNC", layout="wide", initial_sidebar_state="expanded")

# --- ESTILOS CSS MEJORADOS ---
st.markdown("""
    <style>
        .stApp { background-color: #0F172A; color: #F8FAFC; }
        @keyframes latido_critico {
            0% { background-color: #7f1d1d; box-shadow: 0 0 10px #ef4444; }
            50% { background-color: #dc2626; box-shadow: 0 0 25px #ef4444; }
            100% { background-color: #7f1d1d; box-shadow: 0 0 10px #ef4444; }
        }
        .card-normal { background-color: #1e293b; border: 1px solid #334155; }
        .card-riesgo { background-color: #78350f; border: 1px solid #b45309; border-radius: 8px; padding: 15px; margin-bottom: 15px; text-align: center;}
        .card-critico { animation: latido_critico 1s infinite; border: 2px solid #ef4444; border-radius: 8px; padding: 15px; margin-bottom: 15px; text-align: center;}
        .card-offline { background-color: #0f172a; border: 1px dashed #475569; opacity: 0.5; border-radius: 8px; padding: 15px; margin-bottom: 15px; text-align: center;}
    </style>
""", unsafe_allow_html=True)

API_DASHBOARD = "http://backend_api:8000/api/dashboard-tiempo-real"
API_CAOS = "http://backend_api:8000/api/caos"

# ==========================================
# 🍔 MENÚ LATERAL: CONSOLA DEL CAOS
# ==========================================
with st.sidebar:
    st.title("😈 Consola del Caos")
    st.markdown("Inyecta anomalías directo a Kafka para probar la IA en tiempo real.")
    
    maquina_victima = st.selectbox("Selecciona la Víctima:", [f"maq-cnc-{i:02d}" for i in range(1, 51)])
    tipo_falla = st.radio("Tipo de Falla:", ["Falla Térmica", "Desalineación (Vibración)", "Cortocircuito"])
    
    if st.button("💥 INYECTAR FALLA", type="primary", use_container_width=True):
        try:
            payload = {"id_maquina": maquina_victima, "tipo_falla": tipo_falla}
            res = requests.post(API_CAOS, json=payload)
            if res.status_code == 200:
                st.success(f"¡Anomalía enviada a {maquina_victima}!")
                # Le damos un mini sleep para que Spark alcance a procesar antes del re-render
                time.sleep(0.5) 
            else:
                st.error("Error al inyectar falla.")
        except Exception as e:
            st.error("Error de conexión con el Backend API.")

# ==========================================
# 🖥️ PANEL PRINCIPAL: GRILLA FIJA DE MÁQUINAS
# ==========================================
st.title("🏭 Planta de Producción CNC - Vista en Vivo")
st.markdown("Visualización en tiempo real del estado de las 50 máquinas.")

# Contenedor para las métricas y la grilla
metrics_placeholder = st.empty()
grid_placeholder = st.empty()

try:
    response = requests.get(API_DASHBOARD).json()
    
    if response.get("success") and response.get("data"):
        df = pd.DataFrame(response.get("data", []))
        
        # Agrupamos para obtener estrictamente el último estado de cada máquina
        df_latest = df.sort_values('timestamp_lectura').groupby('id_maquina').tail(1)
        # Lo convertimos a diccionario indexado por id_maquina para búsquedas instantáneas
        dict_maquinas = df_latest.set_index('id_maquina').to_dict(orient='index')

        
        cnc_sanas = len(df_latest[df_latest['estado_maquina'] == 'NORMAL'])
        cnc_riesgo = len(df_latest[df_latest['estado_maquina'] == 'RIESGO: REVISAR'])
        cnc_criticas = len(df_latest[df_latest['estado_maquina'] == 'CRITICO: PARADA']) # CORREGIDO
        
        with metrics_placeholder.container():
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Máquinas Detectadas", f"{len(df_latest)} / 50")
            col2.metric("🟢 Normal", cnc_sanas)
            col3.metric("🟡 En Riesgo", cnc_riesgo)
            col4.metric("🔴 Críticas", cnc_criticas)
            st.markdown("---")
            
        # --- RENDERIZADO DE GRILLA FIJA (Evita saltos de posiciones) ---
        with grid_placeholder.container():
            columnas_grilla = st.columns(5) # Grilla limpia de 5 columnas
            
            for i in range(1, 51):
                id_buscado = f"maq-cnc-{i:02d}"
                col_actual = columnas_grilla[(i - 1) % 5]
                
                # Si la máquina existe en las lecturas de la base de datos
                if id_buscado in dict_maquinas:
                    datos_maq = dict_maquinas[id_buscado]
                    estado = datos_maq['estado_maquina']
                    temp = datos_maq['delta_temp']
                    falla = datos_maq['probabilidad_falla_pct']
                    
                    if estado == 'NORMAL':
                        clase_css = "card-normal"
                        icono = "🟢"
                    elif estado == 'RIESGO: REVISAR':
                        clase_css = "card-riesgo"
                        icono = "⚠️"
                    else:
                        clase_css = "card-critico"
                        icono = "🚨"
                    
                    tarjeta_html = f"""
                    <div class="{clase_css}" style="border-radius: 8px; padding: 15px; margin-bottom: 15px; text-align: center;">
                        <h4 style="margin: 0; font-size: 16px;">{id_buscado}</h4>
                        <p style="margin: 5px 0; font-weight: bold; font-size: 13px;">{icono} {estado}</p>
                        <p style="margin: 0; font-size: 12px; color: #cbd5e1;">
                            Δ Temp: {temp}°C<br>
                            Falla: {falla}
                        </p>
                    </div>
                    """
                else:
                    # Si la máquina aún no ha reportado ninguna telemetría
                    tarjeta_html = f"""
                    <div class="card-offline">
                        <h4 style="margin: 0; font-size: 16px; color: #64748b;">{id_buscado}</h4>
                        <p style="margin: 5px 0; font-weight: bold; font-size: 13px; color: #64748b;">⚪ OFFLINE</p>
                        <p style="margin: 0; font-size: 12px; color: #475569;">Sin datos en vivo</p>
                    </div>
                    """
                
                with col_actual:
                    st.markdown(tarjeta_html, unsafe_allow_html=True)
    else:
        st.warning("Conectado a la API, pero la base de datos está vacía...")

except Exception as e:
    st.error(f"Esperando conexión con el servicio Backend... (Detalle: {e})")

# Control de refresco nativo y elegante (Cada 2 segundos recarga el script limpiamente)
time.sleep(2)
st.rerun()