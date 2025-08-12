from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, lower, trim, from_csv, to_date, datediff, lit, concat_ws, udf, from_csv
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark=SparkSession.builder\
    .appName("FlightStreaming_Cleaner")\
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema=StructType([
    StructField("date", StringType()),
    StructField("airline", StringType()),
    StructField("ch_code", StringType()),
    StructField("num_code", StringType()),
    StructField("dep_time", StringType()),
    StructField("from", StringType()),
    StructField("time_taken", StringType()),
    StructField("stop", StringType()),
    StructField("arr_time", StringType()),
    StructField("to", StringType()),
    StructField("price", StringType())])

raw_df=spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "flight-business")\
    .option("startingOffsets", "earliest")\
    .load()

value_df=raw_df.selectExpr("CAST(value AS STRING)")
schema_str="date STRING, airline STRING, ch_code STRING, num_code STRING, dep_time STRING, from STRING, time_taken STRING, stop STRING, arr_time STRING, to STRING, price STRING"
csv_df=value_df.select(from_csv(col("value"), schema_str).alias("data")).select("data.*")

def parse_duration_udf(duration):
    duration=duration.replace("h", "h ").replace("m", "m ")
    parts=duration.strip().split()
    hours=minutes=0
    for part in parts:
        if "h" in part:
            hours=int(part.replace("h", ""))
        elif "m" in part:
            minutes=int(part.replace("m", ""))
    return round(hours + minutes / 60, 2)
parse_duration=udf(parse_duration_udf, FloatType())

def get_time_of_day(dep):
    try:
        hour=int(dep.split(":")[0])
        if 4 <= hour < 8:
            return "Early_Morning"
        elif 8 <= hour < 12:
            return "Morning"
        elif 12 <= hour < 16:
            return "Afternoon"
        elif 16 <= hour < 20:
            return "Evening"
        elif 20 <= hour <= 23:
            return "Night"
        else:
            return "Late_Night"
    except:
        return "Unknown"
time_of_day=udf(get_time_of_day, StringType())

cleaned_df=csv_df.withColumn("price", regexp_replace(col("price"), ",", ""))\
    .withColumn("price", col("price").cast("int"))\
    .withColumn("stop", lower(trim(regexp_replace(col("stop"), "\s+", " "))))\
    .withColumn("stop", regexp_replace(col("stop"), r" stop via .*", " stop"))\
    .withColumn("stop", regexp_replace(col("stop"), r"-stop via .*", "-stop"))\
    .replace({
        "non-stop": "zero",
        "0 stop": "zero",
        "0-stop":"zero",
        "1-stop": "one",
        "1 stop": "one",
        "2-stop": "two_or_more",
        "2+stop": "two_or_more",
        "2 stop": "two_or_more",
        "3 stop": "two_or_more",
        "3-stop": "two_or_more",
        "4 stop": "two_or_more",
        "4-stop": "two_or_more"
    }, subset=["stop"])\
    .withColumn("departure_time", time_of_day(col("dep_time")))\
    .withColumn("arrival_time", time_of_day(col("arr_time")))\
    .withColumn("duration", parse_duration(col("time_taken")))\
    .withColumn("date_parsed", to_date(col("date"), "dd-MM-yyyy"))\
    .withColumn("days_left", datediff(col("date_parsed"),lit("2022-02-10")))\
    .withColumn("flight", concat_ws("-", col("ch_code"), col("num_code")))\
    .withColumn("class", lit("Business"))\
    .withColumn("source_city", col("from"))\
    .withColumn("destination_city", col("to"))\
    .select("airline", "flight", "source_city", "departure_time", col("stop").alias("stops"), "arrival_time", "destination_city", "class", "duration", "days_left", "price")

query=cleaned_df.writeStream\
    .outputMode("append")\
    .format("csv")\
    .option("header", "true")\
    .option("path", "hdfs://localhost:9000/user/sunbeam/cleaned_flight_data_business")\
    .option("checkpointLocation", "hdfs://localhost:9000/user/sunbeam/chkpt_business")\
    .trigger(processingTime="10 seconds")\
    .start()
query.awaitTermination()