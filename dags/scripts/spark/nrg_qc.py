import argparse
import json
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark import SparkContext
from nrg_sql_queries import  null_checker, count_rows, net_gen_qc

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark

def data_qc(spark, input_data, output_data):
    ba_data = input_data + "/bal_auth"
    weather_data = input_data + "/weather"

    df_ba = spark.read.parquet(ba_data)
    df_ba.createOrReplaceTempView("balancing_authorities")

    df_weather = spark.read.parquet(weather_data)
    df_weather.createOrReplaceTempView("weather")

    quality_checks = [{'check_sql': count_rows("balancing_authorities"), 'equal_flag':False,'expected_result': 0}, \
                {'check_sql': null_checker("balancing_authorities", "bal_auth"), 'equal_flag':True,'expected_result': 0}, \
                {'check_sql': net_gen_qc, 'equal_flag':True,'expected_result': 0}, \
                {'check_sql': count_rows("weather"), 'equal_flag':False,'expected_result': 0}, \
                {'check_sql': null_checker("weather", "station_id"), 'equal_flag':True,'expected_result': 0}, \
                {'check_sql': null_checker("weather", "date"), 'equal_flag':True,'expected_result': 0}, \
                {'check_sql': null_checker("weather", "TMIN"), 'equal_flag':True,'expected_result': 0}, \
                {'check_sql': null_checker("weather", "TMAX"), 'equal_flag':True,'expected_result': 0}]

    error_count = 0
    errors_list = []
    for check in quality_checks:
        check_sql = check.get('check_sql')
        expected_result = check.get('expected_result')
        equal = check.get('equal_flag')
        actual_result = spark.sql(check_sql).collect()
        if equal:
            if expected_result != actual_result[0]:
                error_count+=1
                check['actual_result'] = actual_result[0]
                errors_list.append(check)
        else:
            if expected_result == actual_result[0]:
                error_count+=1
                check['actual_result'] = actual_result[0]
                errors_list.append(check)

    with open(output_data + "/data_quality/data_quality.json", 'w') as fout:
        json.dump(errors_list, fout)

def main():
    spark = create_spark_session()

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="hdfs:///output_data")
    parser.add_argument("--output", type=str, help="HDFS output", default="hdfs:///output_data")
    args = parser.parse_args()

    data_qc(spark, input_data=args.input, output_data=args.output)

    spark.stop()

if __name__ == "__main__":
    main()
