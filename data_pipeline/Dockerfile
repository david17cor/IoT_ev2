FROM python:3.11-slim

# Instalar dependencias del sistema necesarias para Java y librerías de Python
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jre \
    gcc \
    libpq-dev \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Establecer el directorio de trabajo
WORKDIR /app

# Instalar PySpark y las dependencias necesarias para tu lógica
RUN pip install --no-cache-dir \
    python-dotenv \
    pyspark==3.5.1 \
    pandas \
    joblib \
    psycopg2-binary \
    sqlalchemy \
    scikit-learn

# Copiar el script de streaming y el modelo entrenado
COPY spark_streaming_limpio.py /app/
COPY modelo_v2_cnc.pkl /app/

# Ejecutar el script directamente con Python
# Asegúrate de que las variables de entorno estén bien cargadas en el docker-compose
CMD ["python", "spark_streaming_limpio.py"]