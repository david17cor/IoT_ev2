from fastapi import FastAPI
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="DataOps Pipelines API",
    description="API intermedia para arquitecturas Medallón (Capas Bronce y Oro).",
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

# CREDENCIALES CAPA ORO
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

# CREDENCIALES CAPA BRONCE
DB_USER2 = os.getenv("DB_USER2", "postgres")
DB_PASSWORD2 = os.getenv("DB_PASSWORD2", "postgres")
DB_HOST2 = os.getenv("DB_HOST2", "postgres_raw")
DB_PORT2 = os.getenv("DB_PORT2", "5432")
DB_NAME2 = os.getenv("DB_NAME2", "postgres")

DATABASE_URL_GOLD = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATABASE_URL_BRONZE = f"postgresql://{DB_USER2}:{DB_PASSWORD2}@{DB_HOST2}:{DB_PORT2}/{DB_NAME2}"

engine_gold = sqlalchemy.create_engine(DATABASE_URL_GOLD)
engine_bronze = sqlalchemy.create_engine(DATABASE_URL_BRONZE)

@app.get("/")
def home():
    return {"status": "API Operativa", "endpoints": ["/api/telemetria", "/api/consulta-cruda"]}

@app.get("/api/telemetria")
def obtener_telemetria_limpia():
    try:
        # 1. Obtenemos las últimas 20 filas para la tabla visual
        query = "SELECT * FROM telemetria_limpia ORDER BY timestamp_lectura DESC LIMIT 20"
        df = pd.read_sql(query, engine_gold)
        if 'timestamp_lectura' in df.columns:
            df['timestamp_lectura'] = df['timestamp_lectura'].astype(str)
        df = df.dropna()
        datos = df.to_dict(orient="records")
        
        # 2. Obtenemos el TOTAL REAL de la base de datos
        total_records = pd.read_sql("SELECT COUNT(*) FROM telemetria_limpia", engine_gold).iloc[0, 0]
        
        return {"success": True, "count": len(datos), "total_db": int(total_records), "data": datos}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/consulta-cruda")
def obtener_telemetria_cruda():
    try:
        # 1. Obtenemos las últimas 20 filas para la tabla visual
        query = "SELECT * FROM raw_records ORDER BY \"timestamp_lectura\" DESC LIMIT 20"
        df = pd.read_sql(query, engine_bronze)
        if 'timestamp_lectura' in df.columns:
            df['timestamp_lectura'] = df['timestamp_lectura'].astype(str)
        datos = df.to_dict(orient="records")
        
        # 2. Obtenemos el TOTAL REAL de la base de datos
        total_records = pd.read_sql("SELECT COUNT(*) FROM raw_records", engine_bronze).iloc[0, 0]
        
        return {"success": True, "count": len(datos), "total_db": int(total_records), "data": datos}
    except Exception as e:
        return {"success": False, "error": str(e)}