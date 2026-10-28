# Informe final: Proyecto Big Data con Apache Kafka y Apache Spark

## 1. Introducción

El proyecto implementa una solución de procesamiento de datos ambientales mediante tecnologías Big Data. Se desarrollaron dos flujos complementarios: procesamiento en tiempo real de mediciones simuladas con Apache Kafka y Spark Structured Streaming, y procesamiento batch de registros históricos de temperatura y humedad con PySpark.

El propósito es mostrar cómo se pueden recibir, transformar, analizar y visualizar datos de sensores para obtener información descriptiva sobre las variables ambientales.

## 2. Objetivos

### Objetivo general

Implementar una solución de procesamiento batch y streaming para analizar datos de temperatura y humedad utilizando Apache Kafka, Apache Spark y Python.

### Objetivos específicos

- Configurar Kafka para recibir mediciones de sensores.
- Desarrollar un productor Python que genere datos simulados.
- Procesar los mensajes en tiempo real mediante Spark Structured Streaming.
- Leer y limpiar datos históricos con PySpark.
- Calcular estadísticas descriptivas generales y por sensor.
- Generar gráficas de la evolución diaria de temperatura y humedad.
- Publicar el código y los resultados seleccionados en GitHub.

## 3. Tecnologías utilizadas

- Apache Kafka 4.4.0, configurado en modo KRaft.
- Apache Spark y PySpark 3.5.8.
- Hadoop 3.3.6 instalado en el entorno.
- Python 3.10.
- Java 11 para Spark y Java 17 para Kafka.
- Pandas y Matplotlib para la visualización de resultados.
- Git y GitHub para el control de versiones y la publicación del código.

## 4. Arquitectura de la solución

### 4.1. Procesamiento en tiempo real

El productor `kafka_producer.py` genera mediciones simuladas con identificador de sensor, temperatura, humedad y fecha. Los mensajes se envían al tópico `sensor_data` de Kafka.

El programa `spark_streaming_consumer.py` consume los mensajes del tópico, convierte los valores JSON a columnas tipadas y calcula promedios de temperatura, promedios de humedad y cantidad de mediciones por sensor en ventanas de un minuto.

Los resultados se muestran en la consola de Spark. La configuración utiliza un intervalo de procesamiento de diez segundos y un checkpoint para mantener el estado de la consulta.

### 4.2. Procesamiento batch

Para el análisis histórico se utilizó el conjunto de datos *Indoor Temperature and Relative Humidity Dataset*, versión 2, publicado en Mendeley Data.

Fuente: https://data.mendeley.com/datasets/dxyvxk6h96/2

Los archivos de texto contienen tres campos: timestamp en milisegundos, humedad relativa y temperatura. Los programas de PySpark leen los registros, convierten los tipos de datos, aplican criterios de validación y calculan estadísticas descriptivas.

Se generaron resultados generales, resúmenes por sensor y mes, un resumen diario y dos gráficas.

## 5. Metodología

1. Instalación y configuración del entorno de trabajo.
2. Configuración de Kafka en modo KRaft y creación del tópico `sensor_data`.
3. Desarrollo y ejecución del productor de datos simulados.
4. Implementación y prueba del consumidor Spark Structured Streaming.
5. Descarga y extracción del conjunto de datos histórico.
6. Lectura, conversión y validación de los registros con PySpark.
7. Cálculo de estadísticas generales y por sensor.
8. Elaboración del resumen diario y generación de gráficas.
9. Organización del código y los resultados seleccionados en GitHub.

## 6. Resultados del procesamiento histórico

El procesamiento inicial informó los siguientes resultados:

- Registros leídos: 1.388.089.
- Registros válidos: 1.388.087.
- Registros descartados: 2.
- Temperatura promedio global: 22,31 °C.
- Temperatura mínima registrada: 14,4 °C.
- Temperatura máxima registrada: 30,3 °C.
- Humedad relativa promedio global: 59,61 %.
- Humedad mínima registrada: 35,1 %.
- Humedad máxima registrada: 86,2 %.

Los criterios de limpieza conservaron registros con humedad entre 0 y 100 % y temperatura entre -10 y 60 °C, siempre que los campos necesarios fueran válidos.

### 6.1. Distribución temporal

La verificación de timestamps encontró registros desde el 1 de octubre de 2019 hasta el 2 de diciembre de 2019.

La distribución por mes real de la fecha fue:

- Octubre de 2019: 694.938 registros.
- Noviembre de 2019: 666.881 registros.
- Diciembre de 2019: 26.270 registros.

Por tanto, el análisis temporal se basa en la fecha real de cada medición y no únicamente en el nombre de la carpeta de origen.

### 6.2. Visualización

Se generaron las siguientes gráficas a partir del resumen diario:

- `resultados/graficas/temperatura_promedio_diaria.png`
- `resultados/graficas/humedad_promedio_diaria.png`

Estas visualizaciones permiten observar la evolución diaria de ambas variables durante el periodo cubierto por los registros.

## 7. Organización del código

Los principales programas desarrollados son:

- `codigo/kafka_producer.py`: generación y envío de mediciones simuladas.
- `codigo/spark_streaming_consumer.py`: consumo y agregación de datos en tiempo real.
- `codigo/analisis_batch.py`: procesamiento y estadísticas de los datos históricos.
- `codigo/resumen_diario.py`: cálculo de estadísticas por fecha.
- `codigo/verificar_fechas.py`: verificación del rango temporal.
- `codigo/graficas_historicas.py`: generación de las gráficas.

Los resultados CSV y las imágenes se encuentran en la carpeta `resultados/`.

## 8. Repositorio

El código y los resultados seleccionados se publicaron en el siguiente repositorio:

https://github.com/D1an4-Ar1st1/proyecto-bigdata-kafka-spark

El repositorio contiene los programas Python, el README, las reglas de exclusión de Git y los resultados gráficos y tabulares seleccionados. Los datos históricos completos y el ZIP original se mantienen fuera del repositorio.

## 9. Conclusiones

El proyecto permitió implementar y probar dos modalidades de procesamiento de datos. En el flujo de streaming, Kafka recibió las mediciones simuladas y Spark Structured Streaming calculó estadísticas en ventanas temporales. En el flujo batch, PySpark procesó más de un millón de registros históricos y generó resúmenes estadísticos y visualizaciones.

Los resultados muestran cómo las herramientas de procesamiento de datos permiten transformar registros de sensores en información descriptiva. La separación entre el flujo en tiempo real y el histórico facilita demostrar ambos enfoques en un mismo proyecto.

Como trabajo futuro, se puede ampliar la validación de calidad, comparar las variables por sensor y periodo, incorporar alertas ante valores definidos como críticos y automatizar la generación de reportes.

## 10. Consideraciones

Las mediciones generadas por el productor de Kafka son simuladas y deben distinguirse de los registros históricos reales. Los promedios y rangos presentados corresponden al conjunto histórico procesado y a los criterios de limpieza implementados.

La instalación de Hadoop en el entorno no implica, por sí sola, que el procesamiento haya sido ejecutado de forma distribuida mediante HDFS. La ejecución documentada en este proyecto corresponde a los programas de Spark que se probaron en la máquina virtual.
