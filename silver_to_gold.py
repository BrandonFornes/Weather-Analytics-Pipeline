import os
import sys
import platform
import pyspark

ruta_proyecto = os.path.dirname(os.path.abspath(__file__))

if platform.system() == "Windows":
    hadoop_dir = os.path.join(ruta_proyecto, "hadoop")
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_dir, "bin")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, to_date, round, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

spark = (SparkSession.builder
    .appName("WeatherGoldReport")
    #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2")
    .config("spark.local.dir", os.path.join(os.path.dirname(os.path.abspath(__file__)), "spark_temp"))
    .config("spark.worker.cleanup.enabled", "false")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

ruta_silver = os.path.join(ruta_proyecto, "data", "silver", "weather")
ruta_gold = os.path.join(ruta_proyecto, "data", "gold", "weather_summary")

print("\n[INFO] Leyendo datos de la Capa Plata (Parquet)...")


esquema_plata_seguro = StructType([
    StructField("ciudad", StringType(), True),
    StructField("temperatura_f", DoubleType(), True),
    StructField("porcentaje_humedad", IntegerType(), True),
    StructField("fecha_ingestion_kafka", TimestampType(), True)
])

# Verificamos simplemente si la carpeta existe físicamente
if os.path.exists(ruta_silver):
    print("[INFO] Ruta detectada. Leyendo Capa Plata con esquema explícito...")
    
    # 🔥 EL TRUCO DE ORO: Agregamos .schema() antes de .parquet()
    # Esto banea el error de "Unable to infer schema" para siempre.
    df_silver = (spark.read
                 .schema(esquema_plata_seguro)
                 .parquet(ruta_silver))
else:
    print("[WARN] La ruta no existe. Creando DataFrame vacío de seguridad...")
    df_silver = spark.createDataFrame([], esquema_plata_seguro)
    df_silver = spark.createDataFrame([], esquema_plata_seguro)

print("[INFO] Calculando métricas y promedios para la Capa Oro...")

df_gold = (df_silver
    .withColumn("fecha", to_date(col("fecha_ingestion_kafka")))
    
    .groupBy("ciudad", "fecha")
    
    .agg(
        round(avg("temperatura_f"), 2).alias("temp_promedio_f"),
        max("temperatura_f").alias("temp_maxima_f"),
        min("temperatura_f").alias("temp_minima_f"),
        round(avg("porcentaje_humedad"), 1).alias("humedad_promedio_porc"),
        max("porcentaje_humedad").alias("humedad_maxima_porc")
    )
    .withColumn("reporte_actualizado_at", current_timestamp())
)

print("\n--- REPORTE EJECUTIVO ANALÍTICO (CAPA ORO) ---")
df_gold.show(10, truncate=False)


print(f"[INFO] Guardando reporte en la Capa Oro física...")

df_gold.write \
    .mode("overwrite") \
    .partitionBy("ciudad") \
    .parquet(ruta_gold)

print(f"Capa Oro guardada en: {ruta_gold}")

spark.stop()
os._exit(0)