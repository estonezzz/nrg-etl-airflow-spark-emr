import argparse
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from nrg_sql_queries import bal_auth_query, weather_query, time_query

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark

def process_bal_auth_data(spark, input_data, output_data):

    # get filepath to balancing authorities data file
    bal_auth_data = input_data + "/*.gz"


    # read balancing authorities data files, apply schema
    df = spark.read.option("header", True).csv(bal_auth_data)
    print(df.count())
    print(bal_auth_query)
    # extract columns to create bal_auth table
    df.createOrReplaceTempView("balancing_authorities")
    bal_auth_table = spark.sql(bal_auth_query)
    print(bal_auth_table.count())
    bal_auth_table.show()

    time_table = spark.sql(time_query)
    time_table.show()
    # write bal_auth table to parquet files partitioned by region, bal_auth, year, month
    #bal_auth_table.write.mode("overwrite").partitionBy('bal_auth', 'year', 'month').parquet(output_data + "/bal_auth.parquet")
    time_table.write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + "/time.parquet")

def process_weather_data(spark, input_data, output_data):

    # get filepath to balancing authorities data file
    weather_data = input_data + "/*.gz"
    locations_data = input_data + "/locations.csv"

    df_loc = spark.read.option("header", True).csv(locations_data)
    df_loc.createOrReplaceTempView("locations")
    locations_list = df_loc.select('Stations').rdd.flatMap(lambda x: x).collect()

    schema = StructType([\
    StructField("station_id", StringType(), True),\
    StructField("date", StringType(), True),\
    StructField("parameter_id", StringType(), True), \
    StructField("value", IntegerType(), True),\
    StructField("m_flag", StringType(), True), \
    StructField("q_flag", StringType(), True), \
    StructField("s_flag", StringType(), True), \
    StructField("time", StringType(), True)])

    df = spark.read.option("header", False).schema(schema).csv(weather_data)
    print(df.count())
    parameters = ["TMIN", "TMAX", "TAVG", "SNOW", "SNWD", "PRCP"]
    df = df.filter(df.station_id.isin(locations_list)) \
    	    .groupBy(["station_id","date"]) \
    	    .pivot("parameter_id", parameters) \
    	    .max("value")
    df.createOrReplaceTempView("weather")
    weather_table = spark.sql(weather_query)
    print(weather_table.count())
    weather_table.show()

    # write weather table to parquet files partitioned by bal_auth, year, month
    weather_table.write.mode("overwrite").partitionBy('bal_auth', 'year', 'month').parquet(output_data + "/weather.parquet")



def main():
    spark = create_spark_session()

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="hdfs:///input_data")
    parser.add_argument("--output", type=str, help="HDFS output", default="hdfs:///output_data")
    args = parser.parse_args()

    process_bal_auth_data(spark, input_data=args.input+"/bal_auth", output_data=args.output+"/bal_auth")
    #process_weather_data(spark, input_data=args.input+"/weather", output_data=args.output+"/weather")


    spark.stop()

if __name__ == "__main__":
    main()
