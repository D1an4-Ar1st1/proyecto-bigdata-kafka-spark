
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, count
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType
)

# 1. Crear la sesión de Spark
spark = (
    SparkSession.builder
    .appName("AnalisisSensoresTiempoReal")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Definir la estructura de los mensajes JSON
schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 3. Leer los mensajes desde Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Convertir el contenido JSON en columnas
parsed_df = (
    raw_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .filter(
        col("sensor_id").isNotNull()
        & col("temperature").isNotNull()
        & col("humidity").isNotNull()
        & col("timestamp").isNotNull()
    )
)

# 5. Agrupar por sensor y ventana de un minuto
windowed_stats = (
    parsed_df
    .withWatermark("timestamp", "2 minutes")
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("sensor_id")
    )
    .agg(
        avg("temperature").alias("temperatura_promedio"),
        avg("humidity").alias("humedad_promedio"),
        count("*").alias("cantidad_mediciones")
    )
)

# 6. Mostrar los resultados en la consola
query = (
    windowed_stats.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation",
            "/home/hadoop/proyecto_bigdata/resultados/checkpoint_streaming")
    .trigger(processingTime="10 seconds")
    .start()
)

print("Spark Streaming está leyendo el topic sensor_data.")

query.awaitTermination()
