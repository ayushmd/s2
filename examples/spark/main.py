from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum, desc

# -------- CONFIG --------
ENDPOINT = "http://127.0.0.1:8000"   # change to your server
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

INPUT_PATH = "s3a://test/sales.csv"      # put a small csv here first
OUTPUT_PATH = "s3a://test/output/sales_agg"    # fresh prefix recommended
# -----------------------

spark = (
    SparkSession.builder
    .appName("s3-demo-basic-aggregations")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    # S3A endpoint/auth
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
             "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
    .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
    # Demo-safe knobs to reduce advanced S3 API usage
    .config("spark.hadoop.fs.s3a.fast.upload", "false")
    .config("spark.hadoop.fs.s3a.multipart.threshold", "1073741824")  # 1GB
    .config("spark.hadoop.fs.s3a.multipart.size", "134217728")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Expect CSV columns like:
# order_id,customer_id,category,amount,quantity,region
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

print("Input sample:")
df.show(10, truncate=False)
df.printSchema()

# Basic cleaning/filtering
clean = (
    df.filter(col("amount").isNotNull())
      .filter(col("quantity").isNotNull())
      .filter(col("amount") > 0)
      .filter(col("quantity") > 0)
)

# Aggregations
agg = (
    clean.groupBy("category", "region")
         .agg(
             count("*").alias("orders"),
             _sum("quantity").alias("total_qty"),
             _sum("amount").alias("total_amount"),
             avg("amount").alias("avg_amount")
         )
         .orderBy(desc("total_amount"))
)

print("Aggregated result:")
agg.show(50, truncate=False)

# For demo reliability: single output file, small dataset
(
    agg.coalesce(1)
       .write
       .mode("overwrite")
       .parquet(OUTPUT_PATH)
)

print(f"Wrote output to: {OUTPUT_PATH}")

# Read back to verify your backend round-trip (glob: commit-style output layout)
roundtrip = spark.read.parquet(OUTPUT_PATH.rstrip("/") + "/*.parquet")
print("Round-trip read:")
roundtrip.show(50, truncate=False)

spark.stop()