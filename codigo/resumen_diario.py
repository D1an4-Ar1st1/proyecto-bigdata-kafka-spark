
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, to_date, avg, min, max, count
)

spark = (
    SparkSession.builder
    .appName("ResumenDiarioSensores")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ruta = "/home/hadoop/proyecto_bigdata/datos/dxyvxk6h96-2/*/*.txt"

datos = (
    spark.read.csv(ruta)
    .withColumnRenamed("_c0", "timestamp_ms")
    .withColumnRenamed("_c1", "humedad")
    .withColumnRenamed("_c2", "temperatura")
    .withColumn("timestamp_ms", col("timestamp_ms").cast("long"))
    .withColumn("humedad", col("humedad").cast("double"))
    .withColumn("temperatura", col("temperatura").cast("double"))
    .withColumn(
        "fecha_hora",
        from_unixtime(col("timestamp_ms") / 1000).cast("timestamp")
    )
    .filter(
        col("timestamp_ms").isNotNull()
        & col("fecha_hora").isNotNull()
        & col("humedad").between(0, 100)
        & col("temperatura").between(-10, 60)
    )
    .withColumn("fecha", to_date("fecha_hora"))
)

resumen = (
    datos
    .groupBy("fecha")
    .agg(
        count("*").alias("mediciones"),
        avg("temperatura").alias("temperatura_promedio"),
        min("temperatura").alias("temperatura_minima"),
        max("temperatura").alias("temperatura_maxima"),
        avg("humedad").alias("humedad_promedio"),
        min("humedad").alias("humedad_minima"),
        max("humedad").alias("humedad_maxima")
    )
    .orderBy("fecha")
)

print("\n===== RESUMEN DIARIO =====")
resumen.show(10, truncate=False)

salida = "/home/hadoop/proyecto_bigdata/resultados/resumen_diario"

(
    resumen.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(salida)
)

print(f"Resumen diario guardado en: {salida}")

spark.stop()
