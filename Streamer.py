

# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import col, from_json, to_timestamp

# InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
baseSchema = StructType([
    StructField("InvoiceNo", StringType(), nullable=False),
    StructField("StockCode", StringType(), nullable=False),
    StructField("Description", StringType(), nullable=True),
    StructField("Quantity", IntegerType(), nullable=False),
    StructField("InvoiceDate", TimestampType(), nullable=True),
    StructField("UnitPrice", FloatType(), nullable=False),
    StructField("CustomerID", IntegerType(), nullable=True),
    StructField("Country", StringType(), nullable=True)
])

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "34.125.248.54:9092") \
        .option("subscribe", "RawData") \
        .load()

parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),baseSchema).alias("data"))

finalDf = parsed_df.selectExpr("data.InvoiceNo AS InvoiceNo",
                 "data.StockCode AS StockCode",
                 "data.Description AS Description",
                 "data.Quantity AS Quantity",
                 "CAST(data.InvoiceDate AS TIMESTAMP) AS InvoiceDate",
                 "data.UnitPrice AS UnitPrice",
                 "data.CustomerID AS CustomerID",
                 "data.Country AS Country")


query = finalDf.writeStream.format("console").start()
#TODO
