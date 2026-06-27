import os
import sys
import pandas as pd
import numpy as np
import joblib

# Obligar a Python a escupir los prints en pantalla
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lower, regexp_replace,
    when, lit, sha2, trim, initcap, concat, substring, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

print("========================================================", flush=True)
print("ARRANCANDO PROCESO DE PIPELINE + TARGET ML V2.0", flush=True)
print("========================================================", flush=True)

# 1. Cargar variables de entorno
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "postgres") 
DB_PORT = os.getenv("DB_PORT", "5432") 
DB_NAME = os.getenv("DB_NAME", "postgres")
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-datalake:9000")

# 2. Cargar el Modelo ML y Estado Global en memoria del Driver
print("Cargando modelo predictivo (Random Forest)...", flush=True)
ruta_modelo = os.path.join("/app", "modelo_v3_cnc.pkl")
modelo_rf = joblib.load(ruta_modelo)
ultimo_estado_maquinas = {}

# 3. Inicializar SparkSession
print("⏳ Inicializando JVM de Spark y descargando Jars...", flush=True)
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
print("🟢 Motor PySpark inicializado correctamente.", flush=True)

# 4. Esquema del sensor IoT V2.0 (Ajustado al nuevo productor)
esquema_sensor = StructType([
    StructField("timestamp_lectura", StringType(), True),
    StructField("id_maquina", StringType(), True),
    StructField("rpm", StringType(), True), # Entra como String por el ruido de comas
    StructField("vibracion_mms", DoubleType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("corriente_motor_a", DoubleType(), True),
    StructField("op_id", StringType(), True),
    StructField("Nombre_Operador", StringType(), True),
    StructField("rut_op", StringType(), True),
    StructField("colapso_fisico_real", BooleanType(), True)
])

# 5. Conectar a Kafka
print("📡 Conectando al Broker de Kafka...", flush=True)
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_broker:9092") \
    .option("subscribe", "telemetria_sucia") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "100") \
    .load()

df_parsed = kafka_stream.select(from_json(col("value").cast("string"), esquema_sensor).alias("data")).select("data.*")

# 6. Función de Procesamiento por Lotes
def process_medallion_batch(batch_df, batch_id):
    global ultimo_estado_maquinas
    print(f"\n [Batch {batch_id}] Iniciando procesamiento de micro-lote...", flush=True)
    batch_df.cache()
    total_crudos = batch_df.count()
    
    if total_crudos > 0:
        # ---   CAPA BRONCE (MinIO) ---
        try:
            df_bronze = batch_df.withColumn("ingest_timestamp", current_timestamp())
            df_bronze.write \
                .format("parquet") \
                .mode("append") \
                .save("s3a://s3bronze/telemetria_raw/")
            print(f"Bronce guardada ({total_crudos} crudos).", flush=True)
        except Exception as e:
            print(f"ERROR CRÍTICO BRONCE: {e}", flush=True)
            
        # --- CAPA PLATA (Limpieza y Anonimización) ---
        df_clean = batch_df \
            .withColumn("timestamp_lectura", to_timestamp(col("timestamp_lectura"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("id_maquina", lower(col("id_maquina"))) \
            .withColumn("rpm", regexp_replace(col("rpm"), ",", ".").cast("float")) \
            .withColumn("nombre_operador", sha2(trim(initcap(col("Nombre_Operador"))), 256)) \
            .withColumn("rut_op", concat(lit("XX.XXX.XX"), substring(col("rut_op"), -3, 3))) \
            .select("timestamp_lectura", "id_maquina", "rpm", "vibracion_mms", "temp_c", 
                    "corriente_motor_a", "op_id", "nombre_operador", "rut_op", "colapso_fisico_real")
                
        df_plata_final = df_clean.filter(col("temp_c").isNotNull() & col("rpm").isNotNull())
        registros_validos = df_plata_final.count()
        
        if registros_validos > 0:
            try:
                # Guardar en BD (Tabla: telemetria_limpia)
                df_plata_final.write.format("jdbc").option("url", JDBC_URL) \
                    .option("dbtable", "telemetria_limpia").option("user", DB_USER) \
                    .option("password", DB_PASSWORD).option("driver", "org.postgresql.Driver") \
                    .mode("append").save()
                print(f"Plata guardada en BD ({registros_validos} limpios).", flush=True)
                
                # --- CAPA ORO (Feature Engineering + Machine Learning) ---
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
                    
                pdf['delta_temp'] = deltas_temp
                pdf['delta_vibracion'] = deltas_vibracion
                pdf['delta_corriente'] = deltas_corriente
                
                # Predicción ML
                features = ['rpm', 'vibracion_mms', 'temp_c', 'corriente_motor_a', 'delta_temp', 'delta_vibracion', 'delta_corriente']
                y_prob = modelo_rf.predict_proba(pdf[features])[:, 1]
                
                pdf['probabilidad_falla'] = y_prob.astype(float)
                
                # --- NUEVA LÓGICA DE NEGOCIO (Data Mart / Capa de Consumo) ---
                # 1. Redondear deltas a 2 decimales
                pdf['delta_temp'] = pdf['delta_temp'].round(2)
                pdf['delta_vibracion'] = pdf['delta_vibracion'].round(2)
                pdf['delta_corriente'] = pdf['delta_corriente'].round(2)
                
                # 🌟 REFORMA ARQUITECTÓNICA: Mapear y redondear lecturas de telemetría reales (Brutas)
                pdf['temp_actual'] = pdf['temp_c'].round(2)
                pdf['vibracion_actual'] = pdf['vibracion_mms'].round(2)
                pdf['corriente_actual'] = pdf['corriente_motor_a'].round(2)
                
                # 2. Convertir la probabilidad a formato porcentaje (ej: "85.5%")
                pdf['probabilidad_falla_pct'] = (pdf['probabilidad_falla'] * 100).round(1).astype(str) + "%"
                
                # 3. Clasificación del Semáforo (Umbrales 40% y 85%, sin emojis)
                condiciones = [
                    pdf['probabilidad_falla'] >= 0.85,
                    pdf['probabilidad_falla'] >= 0.40
                ]
                opciones = ['CRITICO: PARADA', 'RIESGO: REVISAR']
                pdf['estado_maquina'] = np.select(condiciones, opciones, default='NORMAL')
                
                # 🌟 REFORMA ARQUITECTÓNICA: Armamos el DataFrame Final incluyendo los datos brutos
                pdf_dashboard = pdf[[
                    'timestamp_lectura', 'id_maquina', 'delta_temp', 'delta_vibracion', 
                    'delta_corriente', 'estado_maquina', 'probabilidad_falla_pct',
                    'temp_actual', 'vibracion_actual', 'corriente_actual'
                ]]
                
                # Lo regresamos a Spark y lo guardamos en la tabla relacional
                df_dashboard_spark = spark.createDataFrame(pdf_dashboard)
                df_dashboard_spark.write.format("jdbc").option("url", JDBC_URL) \
                    .option("dbtable", "dashboard_tiempo_real").option("user", DB_USER) \
                    .option("password", DB_PASSWORD).option("driver", "org.postgresql.Driver") \
                    .mode("append").save()
                
                print(f"Data Mart guardado en BD (Dashboard). Predicciones y Telemetría actualizadas.", flush=True)
                
            except Exception as e:
                print(f"ERROR CRÍTICO PLATA/ORO: {e}", flush=True)
        else:
            print(f"Sin registros válidos para Plata/Oro tras limpieza.", flush=True)
    else:
        print(f"Lote vacío. Esperando más datos...", flush=True)
            
    batch_df.unpersist()

# 7. Lanzar el Stream
print("Lanzando la consulta de streaming activo...", flush=True)
query = df_parsed.writeStream \
    .foreachBatch(process_medallion_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()