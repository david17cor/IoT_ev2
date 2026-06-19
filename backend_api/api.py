from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import sqlalchemy
import os
import json
import random
from datetime import datetime
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer

app = FastAPI(
    title="DataOps Pipelines API - V2.0 Predictive",
    description="API intermedia para arquitectura IoT y Mantenimiento Predictivo.",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

load_dotenv()

# ==========================================
# CREDENCIALES BASES DE DATOS
# ==========================================
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

DB_USER2 = os.getenv("DB_USER2", "postgres")
DB_PASSWORD2 = os.getenv("DB_PASSWORD2", "postgres")
DB_HOST2 = os.getenv("DB_HOST2", "postgres_raw")
DB_PORT2 = os.getenv("DB_PORT2", "5432")
DB_NAME2 = os.getenv("DB_NAME2", "postgres")

DATABASE_URL_GOLD = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATABASE_URL_BRONZE = f"postgresql://{DB_USER2}:{DB_PASSWORD2}@{DB_HOST2}:{DB_PORT2}/{DB_NAME2}"

engine_gold = sqlalchemy.create_engine(DATABASE_URL_GOLD)
engine_bronze = sqlalchemy.create_engine(DATABASE_URL_BRONZE)

# ==========================================
# CONFIGURACIÓN DE KAFKA (Para Consola del Caos)
# ==========================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetria_sucia") # Ajusta al nombre real de tu tópico

# Inicializamos el productor de Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Pequeño timeout para no bloquear la API si Kafka no está disponible
        api_version_auto_timeout_ms=3000 
    )
except Exception as e:
    print(f"⚠️ Advertencia: No se pudo conectar a Kafka. La Consola del Caos podría fallar. Error: {e}")
    producer = None

# ==========================================
# MODELOS DE DATOS (PYDANTIC)
# ==========================================
class PeticionCaos(BaseModel):
    id_maquina: str
    tipo_falla: str

# ==========================================
# ENDPOINTS GENERALES
# ==========================================
@app.get("/")
def home():
    return {
        "status": "API Operativa", 
        "endpoints": [
            "/api/dashboard-tiempo-real",
            "/api/caos",
            "/api/telemetria", 
            "/api/consulta-cruda"
        ]
    }

# ==========================================
# NUEVO: CONSOLA DEL CAOS (Inyección Kafka)
# ==========================================
@app.post("/api/caos")
def inyectar_falla(peticion: PeticionCaos):
    if not producer:
        raise HTTPException(status_code=500, detail="El Productor de Kafka no está conectado.")
        
    # 1. Valores base (Normales)
    telemetria_maliciosa = {
        "id_maquina": peticion.id_maquina,
        "timestamp_lectura": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "rpm": random.uniform(1200, 1500),
        "temp_c": random.uniform(40, 60),
        "vibracion_mms": random.uniform(1, 3),
        "corriente_motor_a": random.uniform(10, 15)
    }

    # 2. Corrompemos los datos según el tipo de falla seleccionado en el Frontend
    if peticion.tipo_falla == "Falla Térmica":
        telemetria_maliciosa["temp_c"] = random.uniform(110, 150) # Temperatura extrema
    elif peticion.tipo_falla == "Desalineación (Vibración)":
        telemetria_maliciosa["vibracion_mms"] = random.uniform(15, 30) # Vibración destructiva
    elif peticion.tipo_falla == "Cortocircuito":
        telemetria_maliciosa["corriente_motor_a"] = random.uniform(50, 80) # Pico de corriente
        
    try:
        # 3. Enviamos el dato envenenado a Kafka
        producer.send(KAFKA_TOPIC, telemetria_maliciosa)
        producer.flush()
        
        return {
            "success": True, 
            "message": f"Falla '{peticion.tipo_falla}' inyectada con éxito en {peticion.id_maquina}.",
            "payload_enviado": telemetria_maliciosa
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error enviando mensaje a Kafka: {str(e)}")

# ==========================================
# ENDPOINT V2.0 - DASHBOARD EN VIVO
# ==========================================
@app.get("/api/dashboard-tiempo-real")
def obtener_dashboard_predictivo():
    try:
        query = "SELECT * FROM dashboard_tiempo_real ORDER BY timestamp_lectura DESC LIMIT 500"
        df = pd.read_sql(query, engine_gold)
        
        if 'timestamp_lectura' in df.columns:
            df['timestamp_lectura'] = df['timestamp_lectura'].astype(str)
            
        df = df.dropna()
        datos = df.to_dict(orient="records")
        
        return {"success": True, "count": len(datos), "data": datos}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==========================================
# ENDPOINTS V1.2 (Conservados por compatibilidad)
# ==========================================
@app.get("/api/telemetria")
def obtener_telemetria_limpia():
    try:
        query = "SELECT * FROM telemetria_limpia ORDER BY timestamp_lectura DESC LIMIT 20"
        df = pd.read_sql(query, engine_gold)
        if 'timestamp_lectura' in df.columns:
            df['timestamp_lectura'] = df['timestamp_lectura'].astype(str)
        df = df.dropna()
        datos = df.to_dict(orient="records")
        
        total_records = pd.read_sql("SELECT COUNT(*) FROM telemetria_limpia", engine_gold).iloc[0, 0]
        return {"success": True, "count": len(datos), "total_db": int(total_records), "data": datos}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/consulta-cruda")
def obtener_telemetria_cruda():
    try:
        query = "SELECT * FROM raw_records ORDER BY \"timestamp_lectura\" DESC LIMIT 20"
        df = pd.read_sql(query, engine_bronze)
        if 'timestamp_lectura' in df.columns:
            df['timestamp_lectura'] = df['timestamp_lectura'].astype(str)
        datos = df.to_dict(orient="records")
        
        total_records = pd.read_sql("SELECT COUNT(*) FROM raw_records", engine_bronze).iloc[0, 0]
        return {"success": True, "count": len(datos), "total_db": int(total_records), "data": datos}
    except Exception as e:
        return {"success": False, "error": str(e)}