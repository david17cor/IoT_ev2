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
# CONFIGURACIÓN DE KAFKA (Lazy Initialization)
# ==========================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetria_sucia")

_producer = None

def get_kafka_producer():
    """Patrón Singleton/Lazy Init: Solo se conecta a Kafka cuando se necesita."""
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version_auto_timeout_ms=5000,
                retries=3
            )
            print("✅ Conexión exitosa a Kafka establecida.")
        except Exception as e:
            raise Exception(f"Fallo crítico conectando a Kafka en {KAFKA_BROKER}: {str(e)}")
    return _producer

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
        "endpoints": ["/api/dashboard-tiempo-real", "/api/caos", "/api/telemetria", "/api/consulta-cruda"]
    }

# ==========================================
# CONSOLA DEL CAOS (Inyección Kafka)
# ==========================================
@app.post("/api/caos")
def inyectar_falla(peticion: PeticionCaos):
    try:
        producer = get_kafka_producer()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
    telemetria_maliciosa = {
        "id_maquina": peticion.id_maquina,
        "timestamp_lectura": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "rpm": random.uniform(1200, 1500),
        "temp_c": random.uniform(40, 60),
        "vibracion_mms": random.uniform(1, 3),
        "corriente_motor_a": random.uniform(10, 15)
    }

    if peticion.tipo_falla == "Falla Térmica":
        telemetria_maliciosa["temp_c"] = random.uniform(110, 150)
    elif peticion.tipo_falla == "Desalineación (Vibración)":
        telemetria_maliciosa["vibracion_mms"] = random.uniform(15, 30)
    elif peticion.tipo_falla == "Cortocircuito":
        telemetria_maliciosa["corriente_motor_a"] = random.uniform(50, 80)
        
    try:
        producer.send(KAFKA_TOPIC, telemetria_maliciosa)
        producer.flush()
        
        return {
            "success": True, 
            "message": f"Falla '{peticion.tipo_falla}' inyectada en {peticion.id_maquina}.",
            "payload_enviado": telemetria_maliciosa
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error enviando mensaje a Kafka: {str(e)}")

# ==========================================
# 🌟 ENDPOINT REFORMADO: DASHBOARD TIEMPO REAL
# ==========================================
@app.get("/api/dashboard-tiempo-real")
def obtener_dashboard_predictivo():
    try:
        # Usamos comillas triples para la query SQL, evita problemas de formato
        query = """
            SELECT DISTINCT ON (id_maquina) 
                id_maquina, 
                estado_maquina, 
                temp_actual, 
                vibracion_actual, 
                corriente_actual, 
                delta_temp, 
                probabilidad_falla_pct, 
                timestamp_lectura 
            FROM dashboard_tiempo_real 
            ORDER BY id_maquina, timestamp_lectura DESC;
        """
        
        df = pd.read_sql(query, engine_gold)
        
        if df.empty:
            return {"success": True, "count": 0, "data": []}
            
        if 'timestamp_lectura' in df.columns:
            df['timestamp_lectura'] = df['timestamp_lectura'].astype(str)
            
        # 🌟 HOMOLOGACIÓN DE COLUMNAS (Mapeo de seguridad para blindar el Frontend)
        mapeo_columnas = {
            'temp_c': 'temp_actual',
            'vibracion_mms': 'vibracion_actual',
            'corriente_motor_a': 'corriente_actual'
        }
        df = df.rename(columns={k: v for k, v in mapeo_columnas.items() if k in df.columns})
        
        # 🌟 FILTRADO QUIRÚRGICO
        df = df.dropna(subset=['id_maquina', 'estado_maquina'])
        
        # Reemplazamos NaNs restantes por None
        df = df.where(pd.notnull(df), None)
        
        return {"success": True, "count": len(df), "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==========================================
# OTROS ENDPOINTS
# ==========================================
@app.get("/api/telemetria")
def obtener_telemetria_limpia():
    try:
        query = "SELECT * FROM telemetria_limpia ORDER BY timestamp_lectura DESC LIMIT 20"
        df = pd.read_sql(query, engine_gold)
        if 'timestamp_lectura' in df.columns:
            df['timestamp_lectura'] = df['timestamp_lectura'].astype(str)
            
        df = df.dropna(subset=['id_maquina'])
        df = df.where(pd.notnull(df), None)
        return {"success": True, "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/consulta-cruda")
def obtener_telemetria_cruda():
    try:
        query = "SELECT * FROM raw_records ORDER BY timestamp_lectura DESC LIMIT 20"
        df = pd.read_sql(query, engine_bronze)
        if 'timestamp_lectura' in df.columns:
            df['timestamp_lectura'] = df['timestamp_lectura'].astype(str)
        return {"success": True, "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"success": False, "error": str(e)}