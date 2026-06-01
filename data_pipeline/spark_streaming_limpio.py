import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lower, regexp_replace,
    when, lit, sha2, trim, initcap, concat, substring
)
from pyspark.sql.types import StructType, StructField, StringType

# 1. Cargar variables de entorno
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "postgres") 
DB_PORT = os.getenv("DB_PORT", "5432") 
DB_NAME = os.getenv("DB_NAME", "postgres")

# URLs para Capa Oro (Actual) y Capa Bronce (Nueva db_telemetria_cruda)
JDBC_URL_GOLD = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_URL_BRONZE = f"jdbc:postgresql://postgres_raw:5432/telemetria_cruda_db"
print("🔐 Variables de entorno y URLs de conexión preparadas.")

# 2. Inicializar SparkSession
print("⏳ Iniciando motor Apache Spark y descargando dependencias (Kafka + JDBC)...")
spark = SparkSession.builder \
    .appName("DataOps_IoT_Streaming_Medallion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
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
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "telemetria_sucia") \
    .option("startingOffsets", "latest") \
    .load()

# Extraer el JSON
df_parsed = kafka_stream.select(from_json(col("value").cast("string"), esquema_sensor).alias("data")).select("data.*")

# 5. Función de Bifurcación (Capa Bronce y Capa Oro)
def process_medallion_batch(batch_df, batch_id):
    batch_df.cache()
    total_crudos = batch_df.count()
    
    if total_crudos > 0:
        try:
            # --- 🛡️ CAPA BRONCE: Guardar dato 100% crudo ---
            batch_df.write \
                .format("jdbc") \
                .option("url", JDBC_URL_BRONZE) \
                .option("dbtable", "raw_records") \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            # --- 🥇 CAPA ORO: Transformación y Limpieza ---
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
            
            print(f"✅ Batch {batch_id} procesado: {total_crudos} crudos -> {registros_validos} limpios.", flush=True)

        except Exception as e:
            print(f"❌ Error en base de datos al guardar batch {batch_id}: {e}", flush=True)
            
    batch_df.unpersist()

# 6. Ejecutar Stream
query = df_parsed.writeStream \
    .foreachBatch(process_medallion_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()