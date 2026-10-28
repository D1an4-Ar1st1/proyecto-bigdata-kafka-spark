
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, regexp_extract,
    to_timestamp, from_unixtime, avg, min, max,
    count, stddev, hour, month
)

spark = (
    SparkSession.builder
    .appName("AnalisisHistoricoTemperaturaHumedad")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ruta = "/home/hadoop/proyecto_bigdata/datos/dxyvxk6h96-2/*/*.txt"

# Leer archivos de texto: cada fila contiene timestamp, humedad y temperatura.
datos = (
    spark.read
    .option("header", "false")
    .option("inferSchema", "false")
    .csv(ruta)
)

# Identificar los archivos y asignar nombres a las columnas.
datos = (
    datos
    .withColumnRenamed("_c0", "timestamp_ms")
    .withColumnRenamed("_c1", "humedad")
    .withColumnRenamed("_c2", "temperatura")
    .withColumn("archivo", input_file_name())
    .withColumn(
        "sensor_id",
        regexp_extract(col("archivo"), r"([^/]+)\.txt$", 1)
    )
    .withColumn(
        "mes",
        regexp_extract(col("archivo"), r"/(October|November)/", 1)
    )
)

# Convertir tipos de datos.
datos = (
    datos
    .withColumn("timestamp_ms", col("timestamp_ms").cast("long"))
    .withColumn("humedad", col("humedad").cast("double"))
    .withColumn("temperatura", col("temperatura").cast("double"))
    .withColumn(
        "fecha_hora",
        from_unixtime(col("timestamp_ms") / 1000).cast("timestamp")
    )
)

# Conservar registros completos con valores numéricos y rangos plausibles.
limpios = datos.filter(
    col("timestamp_ms").isNotNull()
    & col("fecha_hora").isNotNull()
    & col("humedad").between(0, 100)
    & col("temperatura").between(-10, 60)
    & col("sensor_id").isNotNull()
)

print("\n========== RESUMEN DE CALIDAD ==========")
print(f"Registros leídos: {datos.count()}")
print(f"Registros válidos: {limpios.count()}")
print(f"Registros descartados: {datos.count() - limpios.count()}")

print("\n========== ESQUEMA DE LOS DATOS ==========")
limpios.printSchema()

print("\n========== MUESTRA DE DATOS LIMPIOS ==========")
limpios.select(
    "fecha_hora", "sensor_id", "mes", "humedad", "temperatura"
).show(10, truncate=False)

print("\n========== ESTADÍSTICAS GENERALES ==========")
limpios.select(
    count("*").alias("mediciones"),
    avg("temperatura").alias("temperatura_promedio"),
    min("temperatura").alias("temperatura_minima"),
    max("temperatura").alias("temperatura_maxima"),
    stddev("temperatura").alias("desviacion_temperatura"),
    avg("humedad").alias("humedad_promedio"),
    min("humedad").alias("humedad_minima"),
    max("humedad").alias("humedad_maxima"),
    stddev("humedad").alias("desviacion_humedad")
).show(truncate=False)

print("\n========== ESTADÍSTICAS POR SENSOR Y MES ==========")
resumen = (
    limpios
    .groupBy("mes", "sensor_id")
    .agg(
        count("*").alias("cantidad_mediciones"),
        avg("temperatura").alias("temperatura_promedio"),
        min("temperatura").alias("temperatura_minima"),
        max("temperatura").alias("temperatura_maxima"),
        avg("humedad").alias("humedad_promedio"),
        min("humedad").alias("humedad_minima"),
        max("humedad").alias("humedad_maxima")
    )
    .orderBy("mes", "sensor_id")
)

resumen.show(30, truncate=False)

# Guardar resultados en CSV para documentar y graficar posteriormente.
salida = "/home/hadoop/proyecto_bigdata/resultados/resumen_batch"

(
    resumen.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(salida)
)

print(f"\nResultados guardados en: {salida}")

spark.stop()
