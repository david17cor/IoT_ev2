import json
import hashlib
import os
from datetime import datetime
from kafka import KafkaConsumer
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# 1. Configurar la conexión a PostgreSQL con SQLAlchemy de forma segura
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(DATABASE_URL)
    print("💾 Conexión a PostgreSQL inicializada correctamente.")
except Exception as e:
    print(f"❌ Error al conectar a la Base de Datos: {e}")

# 2. Configurar el Consumidor de Kafka
consumer = KafkaConsumer(
    'telemetria_sucia',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def aplicar_hashing(texto):
    return hashlib.sha256(texto.encode()).hexdigest()

def enmascarar_rut(rut):
    return "XX.XXX.XX" + rut[-3:]

print("🟢 Consumidor DataOps LISTO. Esperando datos para limpiar e insertar en vivo...\n")

for mensaje in consumer:
    dato_sucio = mensaje.value
    
    # Este diccionario DEBE tener las llaves con los mismos nombres exactos de las columnas en DBeaver
    dato_limpio = {}
    
    try:
        # 1. Limpieza de Formato: Arreglar Fechas al estándar ISO
        fecha_obj = datetime.strptime(dato_sucio["timestamp_lectura"], "%d/%m/%Y %H:%M:%S")
        dato_limpio["timestamp_lectura"] = fecha_obj.isoformat()
        
        # 2. Limpieza Estructural: Estandarizar nombre de máquina a minúsculas para mantener consistencia con la API
        dato_limpio["id_maquina"] = str(dato_sucio["ID_Maquina"]).lower()
        
        # 3. Limpieza de Formato: Arreglar comas en números y convertir a Float
        if isinstance(dato_sucio["Revoluciones_RPM"], str):
            rpm_corregido = float(dato_sucio["Revoluciones_RPM"].replace(",", "."))
        else:
            rpm_corregido = float(dato_sucio["Revoluciones_RPM"])
        dato_limpio["rpm"] = rpm_corregido
        
        # 4. Limpieza Semántica: Filtrar temperaturas imposibles
        temp = float(dato_sucio["Temp_C"])
        if temp < 0 or temp > 300:
            dato_limpio["temperatura"] = None  # Al ser FLOAT en Postgres, None se guardará como NULL correctamente
            anomalia_detectada = True
        else:
            dato_limpio["temperatura"] = temp
            anomalia_detectada = False
            
        # 5. Seguridad PII (Cumplimiento Ley 19.628)
        # Mantenemos la lógica de negocio intacta para op_id
        dato_limpio["op_id"] = dato_sucio["op_id"]
        
        nombre_limpio = dato_sucio["Nombre_Operador"].strip().title()
        dato_limpio["nombre_operador"] = aplicar_hashing(nombre_limpio)
        dato_limpio["rut_op"] = enmascarar_rut(dato_sucio["rut_op"])
        
        # --- 6. INSERCIÓN EN TIEMPO REAL A POSTGRESQL ---
        # Convertimos el diccionario limpio en un DataFrame de 1 sola fila
        df_insert = pd.DataFrame([dato_limpio])
        
        # 'append' agrega la fila al final de la tabla 'telemetria_limpia' existente
        df_insert.to_sql('telemetria_limpia', engine, if_exists='append', index=False)
        
        # --- IMPRESIÓN DE DIAGNÓSTICO EN CONSOLA ---
        estado_temp = "⚠️ DESCARTADA (-999)" if anomalia_detectada else f"{dato_limpio['temperatura']}°C"
        
        print("-" * 60)
        print(f"📥 RECIBIDO -> RPM: '{dato_sucio['Revoluciones_RPM']}' | Temp: {dato_sucio['Temp_C']}")
        print(f"💾 GUARDADO -> RPM: {dato_limpio['rpm']} | Temp: {estado_temp} | Tabla: 'telemetria_limpia'")
        
    except Exception as e:
        print(f"❌ Error procesando o guardando el mensaje: {e}")