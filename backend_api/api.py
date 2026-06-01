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
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

# Dos conexiones físicas separadas
DATABASE_URL_GOLD = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATABASE_URL_BRONZE = f"postgresql://{DB_USER}:{DB_PASSWORD}@postgres_raw:5432/telemetria_cruda_db"

engine_gold = sqlalchemy.create_engine(DATABASE_URL_GOLD)
engine_bronze = sqlalchemy.create_engine(DATABASE_URL_BRONZE)

@app.get("/")
def home():
    return {"status": "API Operativa", "endpoints": ["/api/telemetria", "/api/consulta-cruda"]}

@app.get("/api/telemetria")
def obtener_telemetria_limpia():
    try:
        query = "SELECT * FROM telemetria_limpia ORDER BY timestamp_lectura DESC LIMIT 20"
        df = pd.read_sql(query, engine_gold)
        if 'timestamp_lectura' in df.columns:
            df['timestamp_lectura'] = df['timestamp_lectura'].astype(str)
        df = df.dropna()
        datos = df.to_dict(orient="records")
        return {"success": True, "count": len(datos), "data": datos}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/consulta-cruda")
def obtener_telemetria_cruda():
    try:
        # Extraemos los crudos desde la Capa Bronce
        query = "SELECT * FROM raw_records ORDER BY \"timestamp_lectura\" DESC LIMIT 20"
        df = pd.read_sql(query, engine_bronze)
        if 'timestamp_lectura' in df.columns:
            df['timestamp_lectura'] = df['timestamp_lectura'].astype(str)
        datos = df.to_dict(orient="records")
        return {"success": True, "count": len(datos), "data": datos}
    except Exception as e:
        return {"success": False, "error": str(e)}