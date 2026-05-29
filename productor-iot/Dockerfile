# Usamos una imagen ligera de Python
FROM python:3.11-slim

# Establecemos el directorio de trabajo
WORKDIR /app

# Copiamos el script al contenedor
COPY productor_sucio_kafka.py /app/

# Instalamos la librería de Kafka (Ajusta 'kafka-python' si usaste 'confluent-kafka')
RUN pip install --no-cache-dir kafka-python

# Comando para ejecutar el sensor infinito
CMD ["python", "productor_sucio_kafka.py"]