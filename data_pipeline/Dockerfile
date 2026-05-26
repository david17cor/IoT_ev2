FROM bitnami/spark:3.5.1

# Cambiar a usuario root para instalar dependencias
USER root

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar el script y el archivo de requerimientos (si lo hay)
COPY spark_streaming_limpio.py /app/
COPY .env /app/

# Instalar las librerías de Python necesarias para el script
RUN pip install --no-cache-dir python-dotenv pyspark==3.5.1

# Cambiar de nuevo al usuario de Spark por seguridad
USER 1001

# Ejecutar el script usando spark-submit
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "/app/spark_streaming_limpio.py"]