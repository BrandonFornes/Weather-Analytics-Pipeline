import os
import platform
import time

ruta_proyecto = os.path.dirname(os.path.abspath(__file__))

if platform.system() == "Windows":
    print("[INFO] Detectado Windows. Aplicando parche de Hadoop/Winutils...")
    
    hadoop_dir = os.path.join(ruta_proyecto, "hadoop")
    
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_dir, "bin")
else:
    print(f"[INFO] Detectado {platform.system()}. Corriendo Spark de forma nativa.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = (SparkSession.builder
    .appName("WeatherBronzeToSilver")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2")
    .config("spark.local.dir", os.path.join(os.path.dirname(os.path.abspath(__file__)), "spark_temp"))
    .config("spark.worker.cleanup.enabled", "false")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")


df_kafka_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "weather-raw")
    .option("startingOffsets", "earliest")
    .load())

main_schema = StructType([
    StructField("temp", DoubleType(), True),
    StructField("humidity", IntegerType(), True)
])

openweather_schema = StructType([
    StructField("name", StringType(), True),
    StructField("main", main_schema, True)    # Incluimos el sub-objeto "main"
])

df_string = df_kafka_stream.selectExpr("CAST(value AS STRING) as json_puro", "timestamp")
df_parsed = df_string.withColumn("parsed_data", from_json(col("json_puro"), openweather_schema))

df_silver = df_parsed.select(
    col("parsed_data.name").alias("ciudad"),
    col("parsed_data.main.temp").alias("temperatura_f"),
    col("parsed_data.main.humidity").alias("porcentaje_humedad"),
    col("timestamp").alias("fecha_ingestion_kafka")
)

ruta_silver = os.path.join(ruta_proyecto, "data", "silver", "weather")
ruta_checkpoint = os.path.join(ruta_proyecto, "data", "silver", "_checkpoints", "weather")

query = (df_silver.writeStream
    .format("parquet")
    .option("path", ruta_silver)
    .option("checkpointLocation", ruta_checkpoint)
    .outputMode("append")
    .trigger(availableNow=True)
    .start())

query.awaitTermination()


time.sleep(2)
spark.stop()

os._exit(0)