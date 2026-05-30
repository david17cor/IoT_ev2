import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lower, regexp_replace,
    when, lit, sha2, trim, initcap, concat, substring
)
from pyspark.sql.types import StructType, StructField, StringType

# 1. Cargar variables de entorno inyectadas por Docker
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST") 
DB_PORT = os.getenv("DB_PORT") 
DB_NAME = os.getenv("DB_NAME")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
print("🔐 Variables de entorno cargadas:")

# 2. Inicializar SparkSession
# Descargamos dinámicamente los drivers para Kafka y PostgreSQL
print("⏳ Iniciando motor Apache Spark y descargando dependencias (Kafka + JDBC)...")
spark = SparkSession.builder \
    .appName("DataOps_IoT_Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

# Reducir los logs nativos de Spark para limpiar la consola
spark.sparkContext.setLogLevel("WARN")
print("🟢 PySpark iniciado correctamente. Conectando a Kafka...")

# 3. Definir el esquema exacto que envía nuestro sensor IoT
esquema_sensor = StructType([
    StructField("timestamp_lectura", StringType(), True),
    StructField("ID_Maquina", StringType(), True),
    StructField("Revoluciones_RPM", StringType(), True),
    StructField("Temp_C", StringType(), True),
    StructField("op_id", StringType(), True),
    StructField("Nombre_Operador", StringType(), True),
    StructField("rut_op", StringType(), True)
])

# 4. Leer el flujo de datos desde Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "telemetria_sucia") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Transformaciones: Deserializar JSON y aplicar reglas DataOps + Ley 19.628
# Extraemos el 'value' binario de Kafka y lo convertimos a columnas según el esquema
df_parsed = kafka_stream.select(from_json(col("value").cast("string"), esquema_sensor).alias("data")).select("data.*")

df_clean = df_parsed \
    .withColumn("timestamp_lectura", to_timestamp(col("timestamp_lectura"), "dd/MM/yyyy HH:mm:ss")) \
    .withColumn("id_maquina", lower(col("ID_Maquina"))) \
    .withColumn("rpm", regexp_replace(col("Revoluciones_RPM"), ",", ".").cast("float")) \
    .withColumn("temp_num", col("Temp_C").cast("float")) \
    .withColumn("temperatura", when((col("temp_num") >= 0) & (col("temp_num") <= 300), col("temp_num")).otherwise(lit(None).cast("float"))) \
    .withColumn("op_id", col("op_id")) \
    .withColumn("nombre_operador", sha2(trim(initcap(col("Nombre_Operador"))), 256)) \
    .withColumn("rut_op", concat(lit("XX.XXX.XX"), substring(col("rut_op"), -3, 3))) \
    .select("timestamp_lectura", "id_maquina", "rpm", "temperatura", "op_id", "nombre_operador", "rut_op")

# 6. Función para inyectar cada micro-lote (micro-batch) a PostgreSQL
def write_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        try:
            # Filtramos cualquier fila que tenga nulos en columnas críticas para evitar el choque con PostgreSQL
            batch_df_clean = batch_df.filter(col("temperatura").isNotNull() & col("rpm").isNotNull())
            
            # Si después de limpiar aún hay datos, los guardamos
            if not batch_df_clean.isEmpty():
                batch_df_clean.write \
                    .format("jdbc") \
                    .option("url", JDBC_URL) \
                    .option("dbtable", "telemetria_limpia") \
                    .option("user", DB_USER) \
                    .option("password", DB_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                print(f"✅ Micro-batch {batch_id} guardado en DB (Registros válidos: {batch_df_clean.count()})")
            else:
                print(f"⚠️ Micro-batch {batch_id} descartado (Contenía solo datos anómalos/sucios)")
                
        except Exception as e:
            # Si hay un error de conexión o de esquema con la DB, Spark NO muere, solo avisa.
            print(f"❌ Error crítico en base de datos al intentar guardar el batch {batch_id}: {e}")
# 7. Ejecutar el Stream
query = df_clean.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

# Mantener el proceso vivo escuchando nuevos datos
query.awaitTermination()