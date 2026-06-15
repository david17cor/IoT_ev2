import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lower, regexp_replace,
    when, lit, sha2, trim, initcap, concat, substring, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType

# 1. Cargar variables de entorno (Capa Oro)
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "postgres") 
DB_PORT = os.getenv("DB_PORT", "5432") 
DB_NAME = os.getenv("DB_NAME", "postgres")
JDBC_URL_GOLD = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# (NUEVO) Credenciales de MinIO (Capa Bronce)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio_datalake:9000") # Asegúrate que este nombre coincide con tu docker-compose

print("🔐 Variables de entorno Oro (Postgres) y Bronce (MinIO) preparadas.")

if not MINIO_ENDPOINT or MINIO_ENDPOINT.strip() == "":
    MINIO_ENDPOINT = "http://minio_datalake:9000"
elif not MINIO_ENDPOINT.startswith("http://") and not MINIO_ENDPOINT.startswith("https://"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

# 2. Inicializar SparkSession (Con drivers de Kafka, Postgres y AWS S3)
print("⏳ Iniciando motor Apache Spark y descargando dependencias (Kafka + JDBC + AWS S3)...")
spark = SparkSession.builder \
    .appName("DataOps_IoT_Streaming_Medallion_V2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.region", "us-east-1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("🟢 PySpark iniciado. Conectando a Kafka...")

# 3. Esquema del sensor IoT
esquema_sensor = StructType([
    StructField("timestamp_lectura", StringType(), True),
    StructField("ID_Maquina", StringType(), True),
    StructField("Revoluciones_RPM", StringType(), True),
    StructField("Temp_C", StringType(), True),
    StructField("op_id", StringType(), True),
    StructField("Nombre_Operador", StringType(), True),
    StructField("rut_op", StringType(), True)
])

# 4. Leer Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_broker:9092") \
    .option("subscribe", "telemetria_sucia") \
    .option("startingOffsets", "latest") \
    .load()

# Extraer el JSON
df_parsed = kafka_stream.select(from_json(col("value").cast("string"), esquema_sensor).alias("data")).select("data.*")

# 5. Función de Bifurcación (Capa Bronce en S3 y Capa Oro en Postgres)
def process_medallion_batch(batch_df, batch_id):
    batch_df.cache()
    total_crudos = batch_df.count()
    
    if total_crudos > 0:
        # --- 🛡️ CAPA BRONCE: Guardar dato 100% crudo en Data Lake (MinIO) ---
        try:
            # Agregamos una marca de tiempo de cuando llegó al Data Lake
            df_bronze = batch_df.withColumn("ingest_timestamp", current_timestamp())
            
            df_bronze.write \
                .format("parquet") \
                .mode("append") \
                .save("s3a://s3bronze/telemetria_raw/")
                
            print(f"📦 Capa Bronce: {total_crudos} registros guardados en MinIO (S3) en batch {batch_id}.", flush=True)
        except Exception as e:
            print(f"❌ Error crítico en Capa Bronce (MinIO - Batch {batch_id}): {e}", flush=True)
            
        # --- 🥇 CAPA ORO: Transformación y Limpieza (Postgres) ---
        try:
            df_clean = batch_df \
                .withColumn("timestamp_lectura", to_timestamp(col("timestamp_lectura"), "dd/MM/yyyy HH:mm:ss")) \
                .withColumn("id_maquina", lower(col("ID_Maquina"))) \
                .withColumn("rpm", regexp_replace(col("Revoluciones_RPM"), ",", ".").cast("float")) \
                .withColumn("temp_num", col("Temp_C").cast("float")) \
                .withColumn("temperatura", when((col("temp_num") >= 0) & (col("temp_num") <= 300), col("temp_num")).otherwise(lit(None).cast("float"))) \
                .withColumn("op_id", col("op_id")) \
                .withColumn("nombre_operador", sha2(trim(initcap(col("Nombre_Operador"))), 256)) \
                .withColumn("rut_op", concat(lit("XX.XXX.XX"), substring(col("rut_op"), -3, 3))) \
                .select("timestamp_lectura", "id_maquina", "rpm", "temperatura", "op_id", "nombre_operador", "rut_op")
                
            # Filtro de anomalías severas
            df_final = df_clean.filter(col("temperatura").isNotNull() & col("rpm").isNotNull())
            registros_validos = df_final.count()
            
            if registros_validos > 0:
                df_final.write \
                    .format("jdbc") \
                    .option("url", JDBC_URL_GOLD) \
                    .option("dbtable", "telemetria_limpia") \
                    .option("user", DB_USER) \
                    .option("password", DB_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                print(f"🥇 Capa Oro: {registros_validos} registros limpios guardados en Postgres batch {batch_id}.", flush=True)
            else:
                print(f"⚠️ Batch {batch_id} no generó registros válidos para Capa Oro.", flush=True)
        except Exception as e:
            print(f"❌ Error crítico en Capa Oro (Postgres - Batch {batch_id}): {e}", flush=True)
            
    batch_df.unpersist()

# 6. Ejecutar Stream
query = df_parsed.writeStream \
    .foreachBatch(process_medallion_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()