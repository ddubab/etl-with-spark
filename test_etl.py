from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, LongType



# Create a SparkSession
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

# Check spark connection
print(spark.version)

# Define schema for JSON data
schema = StructType([
    StructField("customer_id", LongType()),
    StructField("order_id", LongType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_amount", StringType(), True),
    StructField("product_name", StringType(), True)
])

# Read JSON data into PySpark DataFrame
df = spark.read.json("orders.json", schema=schema)

# Apply data transformations
df_transformed = df.select(
    col("customer_id"),
    year(col("order_date")).alias("order_year"),
    month(col("order_date")).alias("order_month"),
    dayofmonth(col("order_date")).alias("order_day"),
    col("order_amount").cast("float").alias("order_amount"),
    concat_ws("-",col("customer_id"),col("order_id")).alias("order_key"),
    col("product_name")
)

# Write transformed data to MySQL database

url = "jdbc:mysql://db:3306/mydatabase"
table_name = "orders"
mode = "append"
properties = {
 "driver": "com.mysql.cj.jdbc.Driver",
 "user": "root",
 "password": "example"
}

df_transformed.write.jdbc(url=url, table=table_name,mode="append",properties=properties)
df_transformed.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)

#터미널 실행 
##docker exec -it ed6531895de3 spark-submit test_etl.py