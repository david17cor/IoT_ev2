FROM python:3.11-slim

# Instalar dependencias del sistema necesarias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jre \
    gcc \
    libpq-dev \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar el archivo de requerimientos primero (mejora el cache de Docker)
COPY requirements.txt .

# Instalar todas las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código y el modelo
COPY spark_streaming_limpio.py .
COPY modelo_cnc.pkl .

CMD ["python", "spark_streaming_limpio.py"]