# Deployment options => pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import col, from_json, to_json, struct

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

SUBSCRIBE_TOPIC = "RawData"
PUBLISH_TOPIC = "TransformedData"
CHECKPOINT_DIR = "/home/arda_aras_dev/checkpointdir2"
CONSUMER_GROUP_ID = "Transformers"

# Subscribe to RawData topic
df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "34.125.112.207:9092") \
        .option("failOnDataLoss", False) \
        .option("subscribe", SUBSCRIBE_TOPIC) \
        .load()

# Convert JSON and apply dataframe schema
parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),baseSchema).alias("data"))

finalDf = parsed_df.selectExpr("data.InvoiceNo AS InvoiceNo",
                 "data.StockCode AS StockCode",
                 "data.Description AS Description",
                 "data.Quantity AS Quantity",
                 "CAST(data.InvoiceDate AS TIMESTAMP) AS InvoiceDate",
                 "data.UnitPrice AS UnitPrice",
                 "data.CustomerID AS CustomerID",
                 "data.Country AS Country")

# Remove duplicates and NaN values
cleanedFinalDf = finalDf.dropDuplicates().na.drop()

kafkaDf = cleanedFinalDf.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "34.125.112.207:9092") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .option("failOnDataLoss", False) \
    .option("kafka.group.id", CONSUMER_GROUP_ID) \
    .option("topic", PUBLISH_TOPIC) \
    .start()
