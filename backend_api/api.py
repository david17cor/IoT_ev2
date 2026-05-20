from fastapi import FastAPI
import pandas as pd
import sqlalchemy
import numpy as np
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="DataOps Pipelines API",
    description="API intermedia para la extracción de telemetría filtrada y segura.",
    version="1.0.0"
)

# Permitir que el frontend se conecte sin problemas de seguridad (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conexión a la Base de Datos
engine = sqlalchemy.create_engine('postgresql://admin_dataops:password_seguro_123@localhost:5434/iot_predictivo')

@app.get("/")
def home():
    return {"status": "API Operativa", "endpoints": ["/api/telemetria"]}

@app.get("/api/telemetria")
def obtener_telemetria():
    try:
        # Traer los últimos 20 registros procesados
        query = "SELECT * FROM telemetria_limpia ORDER BY timestamp_lectura DESC LIMIT 20"
        df = pd.read_sql(query, engine)
        
        # Convertir el timestamp a string para evitar problemas de serialización JSON
        if 'timestamp_lectura' in df.columns:
            df['timestamp_lectura'] = df['timestamp_lectura'].astype(str)


        # Elimina cualquier registro que contenga al menos un valor nulo/vacío
        df = df.dropna()
            
        # Transformar el DataFrame a un formato JSON compatible con la API
        datos = df.to_dict(orient="records")
        return {"success": True, "count": len(datos), "data": datos}
    except Exception as e:
        return {"success": False, "error": str(e)}