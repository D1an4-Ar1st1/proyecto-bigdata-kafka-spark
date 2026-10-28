
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, min, max, count

spark = (
    SparkSession.builder
    .appName("VerificarFechasHistoricas")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ruta = "/home/hadoop/proyecto_bigdata/datos/dxyvxk6h96-2/*/*.txt"

datos = (
    spark.read
    .csv(ruta)
    .withColumnRenamed("_c0", "timestamp_ms")
    .withColumnRenamed("_c1", "humedad")
    .withColumnRenamed("_c2", "temperatura")
    .withColumn("timestamp_ms", col("timestamp_ms").cast("long"))
    .withColumn(
        "fecha_hora",
        from_unixtime(col("timestamp_ms") / 1000).cast("timestamp")
    )
)

print("\n===== RANGO DE FECHAS DEL CONJUNTO =====")

datos.select(
    min("fecha_hora").alias("fecha_inicial"),
    max("fecha_hora").alias("fecha_final"),
    count("*").alias("registros")
).show(truncate=False)

print("\n===== REGISTROS POR MES REAL DE LA FECHA =====")

from pyspark.sql.functions import date_format

(
    datos
    .filter(col("fecha_hora").isNotNull())
    .groupBy(date_format("fecha_hora", "yyyy-MM").alias("mes_real"))
    .count()
    .orderBy("mes_real")
    .show(truncate=False)
)

spark.stop()

