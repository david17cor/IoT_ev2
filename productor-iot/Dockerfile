FROM python:3.11-slim

WORKDIR /app

# Copiar el archivo de dependencias
COPY requirements.txt .

# Instalar todas las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código
COPY productor_sucio_kafka.py .

CMD ["python", "productor_sucio_kafka.py"]