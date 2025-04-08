from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("WeeklySKUReport").getOrCreate()

df = spark.read.csv("hdfs://localhost:9000/hcmus/asr.csv", header=True, inferSchema=True)

df = df.withColumn("Date", f.to_date(f.col("Date"), "MM-dd-yy"))

df = df.filter(f.col('Status').like('Shipped%'))

df = df.withColumn(
    'report_date',
    f.when(f.dayofweek(f.col('Date')) == 1, f.date_add(f.col('Date'), 1))
     .otherwise(f.date_add(f.date_sub(f.col('Date'), f.dayofweek(f.col('Date'))), 9))
)
results = df.select('report_date', 'SKU', 'Qty').groupBy('report_date', 'SKU').agg(f.sum('Qty').alias('Qty')).orderBy('report_date')

results.coalesce(1).write.option("header", "true").csv("hdfs://localhost:9000/hcmus/output")

spark.stop()
