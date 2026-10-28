
# Proyecto Big Data: Análisis de temperatura y humedad

## 1. Descripción

Este proyecto implementa un flujo de procesamiento de datos en tiempo real e histórico utilizando Apache Kafka y Apache Spark.

Se simulan mediciones de sensores de temperatura y humedad para el procesamiento en streaming. Además, se analizan datos históricos de sensores ambientales mediante PySpark.

## 2. Objetivos

- Implementar la transmisión de datos con Apache Kafka.
- Procesar mediciones en tiempo real con Spark Structured Streaming.
- Leer y limpiar datos históricos con PySpark.
- Calcular estadísticas descriptivas de temperatura y humedad.
- Generar gráficas de evolución diaria de las variables ambientales.

## 3. Tecnologías utilizadas

- Apache Kafka 4.4.0
- Apache Spark / PySpark 3.5.8
- Hadoop 3.3.6
- Python 3.10
- Java 11 para Spark y Java 17 para Kafka
- Pandas y Matplotlib

## 4. Estructura del proyecto

```text
proyecto_bigdata/
├── codigo/
│   ├── analisis_batch.py
│   ├── graficas_historicas.py
│   ├── kafka_producer.py
│   ├── resumen_diario.py
│   ├── spark_streaming_consumer.py
│   └── verificar_fechas.py
├── datos/
│   └── (datos históricos, no incluidos en GitHub)
├── documentacion/
├── resultados/
│   ├── graficas/
│   ├── resumen_batch/
│   └── resumen_diario/
└── README.md
```

## 5. Procesamiento en tiempo real

El productor Python genera mediciones simuladas que incluyen identificador de sensor, temperatura, humedad y fecha.

Las mediciones se envían al tópico `sensor_data` de Kafka. Spark Structured Streaming consume el tópico y calcula promedios y cantidades por sensor en ventanas de un minuto.

## 6. Procesamiento batch

Los datos históricos contienen registros de temperatura y humedad de sensores ambientales.

El procesamiento con PySpark convierte los campos, valida los valores y calcula estadísticas generales, por sensor, por mes y por día.

## 7. Resultados obtenidos

- 1.388.089 registros históricos leídos.
- 1.388.087 registros válidos según los criterios de limpieza utilizados.
- Temperatura promedio global: aproximadamente 22,31 °C.
- Humedad relativa promedio global: aproximadamente 59,61 %.
- Se generaron gráficas diarias de temperatura y humedad.

## 8. Ejecución

Los programas deben ejecutarse con las rutas y versiones de software configuradas en el entorno local.

Productor Kafka:

```bash
python3 /home/hadoop/proyecto_bigdata/codigo/kafka_producer.py
```

Consumidor Spark Streaming:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
/home/hadoop/proyecto_bigdata/codigo/spark_streaming_consumer.py
```

Procesamiento batch:

```bash
spark-submit /home/hadoop/proyecto_bigdata/codigo/analisis_batch.py
```

Resumen diario:

```bash
spark-submit /home/hadoop/proyecto_bigdata/codigo/resumen_diario.py
```

Generación de gráficas:

```bash
python3 /home/hadoop/proyecto_bigdata/codigo/graficas_historicas.py
```

## 9. Fuente de datos históricos

Indoor Temperature and Relative Humidity Dataset, Mendeley Data, versión 2.

https://data.mendeley.com/datasets/dxyvxk6h96/2

Los datos originales deben descargarse por separado y ubicarse en la carpeta `datos/`.

## 10. Resultados visuales

Las gráficas generadas se encuentran en `resultados/graficas/`.
