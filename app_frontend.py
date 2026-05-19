import streamlit as st
import requests
import pandas as pd
import time

st.set_page_config(page_title="DataOps Monitor", layout="wide")

st.title("🖥️ Centro de Control DataOps - Pipeline IoT")
st.markdown("### Extracción de datos mediante API REST intermedia")

# Contenedor para refresco en tiempo real
placeholder = st.empty()

API_URL = "http://127.0.0.1:8000/api/telemetria"

while True:
    try:
        # Consumir la API
        response = requests.get(API_URL).json()
        
        if response.get("success") and response.get("data"):
            data_limpia = response["data"]
            df_limpio = pd.DataFrame(data_limpia)
            
            with placeholder.container():
                # Fila de Métricas Generales
                st.metric(label="Flujo de Ingesta Activo (API)", value=f"{len(df_limpio)} Eventos/s")
                
                st.markdown("---")
                
                # Diseño de Pantalla Dividida: ANTES vs DESPUÉS
                col_antes, col_despues = st.columns(2)
                
                with col_antes:
                    st.error("📥 ANTES: Datos Crutos de la Planta (Simulados en Sensor)")
                    # Recreamos visualmente cómo venía el dato sucio para el contraste
                    df_sucio = df_limpio.copy()
                    if not df_sucio.empty:
                        # Simulamos el desorden original para la comparativa visual
                        df_sucio['rpm'] = df_sucio['rpm'].astype(str).str.replace('.', ',')
                        df_sucio['operador_nombre'] = "   " + df_sucio['id_maquina'].str.upper() + "   " 
                        # Añadimos un par de filas con el -999 simulado para impactar
                        if len(df_sucio) > 2:
                            df_sucio.loc[0, 'temperatura'] = -999.0
                            df_sucio.loc[2, 'temperatura'] = -999.0
                    st.dataframe(df_sucio[['timestamp_lectura', 'id_maquina', 'rpm', 'temperatura']], use_container_width=True)
                    st.caption("🚨 Problemas detectados: Formatos de número incorrectos (comas), nombres con espacios vacíos y lecturas de temperatura fuera de rango (-999°C).")

                with col_despues:
                    st.success("✨ DESPUÉS: Datos Curados y Seguros (DataOps + Ley 19.628)")
                    # Mostramos los datos reales limpios que envió la API, incluyendo las transformaciones PII
                    st.dataframe(df_limpio, use_container_width=True)
                    st.caption("✅ Soluciones aplicadas: Tipos de datos corregidos, remoción de anomalías semánticas, RUT enmascarado y Nombre del operador protegido mediante hashing SHA-256.")

        else:
            st.warning("La API respondió pero no se encontraron datos en la base de datos.")
            
    except Exception as e:
        st.info("🔌 Conectando con la API REST en el puerto 8000... Asegúrate de iniciar api.py")
        
    time.sleep(2)