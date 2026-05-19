import streamlit as st
import requests
import pandas as pd
import time

# Configuración inicial de la página
st.set_page_config(page_title="DataOps Monitor", layout="wide")

# Inyección de CSS para diseño profesional y aumento del tamaño de tablas
st.markdown("""
    <style>
        /* Fondo corporativo sutil */
        .stApp {
            background-color: #f8f9fa;
        }
        /* Aumento del tamaño de fuente general y de las tablas */
        html, body, [class*="css"] {
            font-size: 16px !important;
        }
        /* Contenedores de DataFrames con borde sutil */
        [data-testid="stDataFrame"] {
            border: 1px solid #dee2e6;
            border-radius: 4px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        /* Ajuste de color para los textos descriptivos */
        .stMarkdown caption {
            color: #6c757d !important;
            font-size: 14px !important;
        }
    </style>
""", unsafe_allow_html=True)

st.title("Centro de Control DataOps - Pipeline IoT")
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
                    st.error("ANTES: Datos Crudos de la Planta (Simulados en Sensor)")
                    df_sucio = df_limpio.copy()
                    if not df_sucio.empty:
                        # Simulando los errores de origen
                        df_sucio['rpm'] = df_sucio['rpm'].astype(str).str.replace('.', ',')
                        df_sucio['nombre_operador'] = "   " + df_sucio['id_maquina'].str.upper() + "   " 
                        if len(df_sucio) > 2:
                            df_sucio.loc[0, 'temperatura'] = -999.0
                            df_sucio.loc[2, 'temperatura'] = -999.0
                    
                    # Se muestran los datos con un height fijo para igualar tamaños visuales
                    st.dataframe(df_sucio[['timestamp_lectura', 'id_maquina', 'rpm', 'temperatura']], use_container_width=True, height=400)
                    st.caption("Problemas detectados: Formatos incorrectos (comas), nombres con espacios en blanco y picos de temperatura atípicos (-999.0).")

                with col_despues:
                    st.success("DESPUÉS: Datos Curados y Seguros (DataOps + Ley 19.628)")
                    
                    # Mostrar datos procesados
                    st.dataframe(df_limpio, use_container_width=True, height=400)
                    st.caption("Soluciones aplicadas: Tipado estandarizado, remoción de anomalías, RUT enmascarado y Hashing SHA-256 para anonimización.")
            else:
                st.warning("La API respondió con éxito, pero la tabla 'telemetria_limpia' en PostgreSQL no tiene registros actualmente.")
                st.info("Sugerencia de sistema: Verificar la ejecución del script del Consumidor Kafka y la inserción de filas en la base de datos.")
                
    except Exception as e:
        with placeholder.container():
            st.info("Conectando con la API REST en el puerto 8000... Verificando estado del servicio api.py")
        
    time.sleep(2)