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
        
        # Mover el contenedor al inicio del flujo para que limpie la pantalla en cada ciclo
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
                    st.error("📥 ANTES: Datos Crutos de la Planta (Simulados en Sensor)")
                    df_sucio = df_limpio.copy()
                    if not df_sucio.empty:
                        df_sucio['rpm'] = df_sucio['rpm'].astype(str).str.replace('.', ',')
                        df_sucio['nombre_operador'] = "   " + df_sucio['id_maquina'].str.upper() + "   " 
                        if len(df_sucio) > 2:
                            df_sucio.loc[0, 'temperatura'] = -999.0
                            df_sucio.loc[2, 'temperatura'] = -999.0
                    st.dataframe(df_sucio[['timestamp_lectura', 'id_maquina', 'rpm', 'temperatura']], use_container_width=True)
                    st.caption("🚨 Problemas detectados: Formatos incorrectos (comas), nombres sucios y picos de temperatura (-999°C).")

                with col_despues:
                    st.success("✨ DESPUÉS: Datos Curados y Seguros (DataOps + Ley 19.628)")
                    st.dataframe(df_limpio, use_container_width=True)
                    st.caption("✅ Soluciones aplicadas: Tipos de datos corregidos, remoción de anomalías, RUT enmascarado y Hashing SHA-256.")
            else:
                # Ahora la alerta se sobreescribe y no se duplica
                st.warning("⚠️ La API respondió con éxito, pero la tabla 'telemetria_limpia' en PostgreSQL no tiene registros todavía.")
                st.info("💡 Tip: Revisa si tu script del Consumidor Kafka está encendido e insertando filas en la base de datos.")
                
    except Exception as e:
        with placeholder.container():
            st.info("🔌 Conectando con la API REST en el puerto 8000... Asegúrate de iniciar api.py")
        
    time.sleep(2)