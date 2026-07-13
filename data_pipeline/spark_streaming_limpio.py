import os
import sys
import pandas as pd
import numpy as np
import joblib

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lower, regexp_replace,
    when, lit, sha2, trim, initcap, concat, substring, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

print("========================================================", flush=True)
print("ARRANCANDO PROCESO DE PIPELINE + TARGET ML V3.0 (FIXED)", flush=True)
print("========================================================", flush=True)

# 1. Configuración de variables de entorno
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "postgres") 
DB_PORT = os.getenv("DB_PORT", "5432") 
DB_NAME = os.getenv("DB_NAME", "postgres")
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-datalake:9000")

# 2. Carga del Modelo ML
print("Cargando modelo predictivo (Random Forest)...", flush=True)
ruta_modelo = os.path.join("/app", "modelo_cnc.pkl")
modelo_rf = joblib.load(ruta_modelo)

# Diccionario global para calcular los deltas (estado en memoria)
ultimo_estado_maquinas = {}

# 3. Inicialización de Spark Streaming
print("Inicializando JVM de Spark y descargando Jars...", flush=True)
spark = SparkSession.builder \
    .appName("DataOps_IoT_Streaming_Medallion_ML") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Motor PySpark inicializado correctamente.", flush=True)

# 4. Esquema de Ingesta (Kafka)
esquema_sensor = StructType([
    StructField("timestamp_lectura", StringType(), True),
    StructField("id_maquina", StringType(), True),
    StructField("rpm", StringType(), True),
    StructField("vibracion_mms", DoubleType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("corriente_motor_a", DoubleType(), True),
    StructField("op_id", StringType(), True),
    StructField("Nombre_Operador", StringType(), True),
    StructField("rut_op", StringType(), True),
    StructField("colapso_fisico_real", BooleanType(), True),
    StructField("maquina_inactiva", BooleanType(), True)
])

print("Conectando al Broker de Kafka...", flush=True)
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_broker:9092") \
    .option("subscribe", "telemetria_sucia") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "50") \
    .load()

df_parsed = kafka_stream.select(from_json(col("value").cast("string"), esquema_sensor).alias("data")).select("data.*")

# 5. Función de Procesamiento por Micro-lote (Medallion Architecture)
def process_medallion_batch(batch_df, batch_id):
    global ultimo_estado_maquinas
    print(f"\n[Batch {batch_id}] Iniciando procesamiento de micro-lote...", flush=True)
    
    batch_df.cache()
    total_crudos = batch_df.count()
    
    if total_crudos > 0:
        # --- CAPA BRONCE (Datalake - MinIO) ---
        try:
            df_bronze = batch_df.withColumn("ingest_timestamp", current_timestamp())
            df_bronze.write \
                .format("parquet") \
                .mode("append") \
                .save("s3a://s3bronze/telemetria_raw/")
            print(f"-> Bronce guardada ({total_crudos} crudos).", flush=True)
        except Exception as e:
            print(f"ERROR CRITICO BRONCE: {e}", flush=True)
            
        # --- LIMPIEZA DE DATOS ---
        df_clean = batch_df \
            .withColumn("timestamp_lectura", to_timestamp(col("timestamp_lectura"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("id_maquina", lower(col("id_maquina"))) \
            .withColumn("rpm", regexp_replace(col("rpm"), ",", ".").cast("float")) \
            .withColumn("nombre_operador", sha2(trim(initcap(col("Nombre_Operador"))), 256)) \
            .withColumn("rut_op", concat(lit("XX.XXX.XX"), substring(col("rut_op"), -3, 3))) \
            .select("timestamp_lectura", "id_maquina", "rpm", "vibracion_mms", "temp_c", 
                    "corriente_motor_a", "op_id", "nombre_operador", "rut_op", "colapso_fisico_real", "maquina_inactiva")
                
        df_plata_final = df_clean.filter(col("temp_c").isNotNull() & col("rpm").isNotNull())
        registros_validos = df_plata_final.count()
        
        if registros_validos > 0:
            try:
                # --- CAPA PLATA (Data Warehouse - PostgreSQL) ---
                df_plata_final.write.format("jdbc").option("url", JDBC_URL) \
                    .option("dbtable", "telemetria_limpia").option("user", DB_USER) \
                    .option("password", DB_PASSWORD).option("driver", "org.postgresql.Driver") \
                    .mode("append").save()
                print(f"-> Plata guardada en BD ({registros_validos} limpios).", flush=True)
                
                # --- INFERENCIA ML Y CÁLCULO DE DELTAS (Data Mart) ---
                pdf = df_plata_final.toPandas()
                pdf = pdf.sort_values(by=['id_maquina', 'timestamp_lectura'])
                
                deltas_temp, deltas_vibracion, deltas_corriente = [], [], []
                
                for index, row in pdf.iterrows():
                    maq = row['id_maquina']
                    if maq in ultimo_estado_maquinas:
                        d_temp = row['temp_c'] - ultimo_estado_maquinas[maq]['temp_c']
                        d_vib = row['vibracion_mms'] - ultimo_estado_maquinas[maq]['vibracion']
                        d_corr = row['corriente_motor_a'] - ultimo_estado_maquinas[maq]['corriente']
                    else:
                        d_temp, d_vib, d_corr = 0.0, 0.0, 0.0
                        
                    deltas_temp.append(float(d_temp))
                    deltas_vibracion.append(float(d_vib))
                    deltas_corriente.append(float(d_corr))
                    
                    ultimo_estado_maquinas[maq] = {
                        'temp_c': row['temp_c'],
                        'vibracion': row['vibracion_mms'],
                        'corriente': row['corriente_motor_a']
                    }
                    
                pdf['delta_temp'] = np.round(deltas_temp, 2)
                pdf['delta_vibracion'] = np.round(deltas_vibracion, 2)
                pdf['delta_corriente'] = np.round(deltas_corriente, 2)
                
                features = ['rpm', 'vibracion_mms', 'temp_c', 'corriente_motor_a', 'delta_temp', 'delta_vibracion', 'delta_corriente']
                y_prob = modelo_rf.predict_proba(pdf[features])[:, 1]
                
                pdf['probabilidad_falla'] = y_prob.astype(float)
                
                # Reglas de Negocio para Dashboard
                condiciones = [
                    pdf['maquina_inactiva'] == True,
                    pdf['probabilidad_falla'] >= 0.95,
                    pdf['probabilidad_falla'] >= 0.40
                ]
                opciones = ['INACTIVO', 'CRITICO: PARADA', 'RIESGO: REVISAR']
                pdf['estado_maquina'] = np.select(condiciones, opciones, default='NORMAL')
                
                # Castear porcentaje a String para igualar el esquema de destino en la BD
                pdf.loc[pdf['estado_maquina'] == 'INACTIVO', 'probabilidad_falla'] = 1.0
                pdf['probabilidad_falla_pct'] = (pdf['probabilidad_falla'] * 100).round(0).astype(int).astype(str)
                
                # --- CAPA ORO: FILTRO ESTRICTO DEL ESQUEMA DESTINO ---
                # Se seleccionan SOLO las 7 columnas que la tabla destino requiere.
                pdf_dashboard = pdf[[
                    'timestamp_lectura', 
                    'id_maquina', 
                    'delta_temp', 
                    'delta_vibracion', 
                    'delta_corriente', 
                    'estado_maquina', 
                    'probabilidad_falla_pct'
                ]]
                
                df_dashboard_spark = spark.createDataFrame(pdf_dashboard)
                
                # Escritura al Dashboard (Overwrite)
                df_dashboard_spark.write.format("jdbc").option("url", JDBC_URL) \
                    .option("dbtable", "dashboard_tiempo_real").option("user", DB_USER) \
                    .option("password", DB_PASSWORD).option("driver", "org.postgresql.Driver") \
                    .option("truncate", "true") \
                    .mode("overwrite").save()
                
                print(f"-> Data Mart (Dashboard) sobrescrito con éxito. Máquinas procesadas.", flush=True)
                
            except Exception as e:
                print(f"ERROR CRITICO PLATA/ORO: {e}", flush=True)
        else:
            print(f"Sin registros validos para Plata/Oro tras limpieza.", flush=True)
    else:
        print(f"Lote vacio. Esperando mas datos...", flush=True)
            
    batch_df.unpersist()

# 6. Arranque de la Tubería
print("Lanzando la consulta de streaming activo...", flush=True)
query = df_parsed.writeStream \
    .foreachBatch(process_medallion_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()