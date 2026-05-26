FROM python:3.11-slim

# Instalar Java (Requisito indispensable para que Apache Spark funcione)
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar el script y las variables de entorno
COPY spark_streaming_limpio.py /app/
COPY .env /app/

# Instalar PySpark y dotenv
RUN pip install --no-cache-dir python-dotenv pyspark==3.5.1

# Ejecutar el script directamente con Python
CMD ["python", "spark_streaming_limpio.py"]