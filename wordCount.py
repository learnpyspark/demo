from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("WordCount") \
        .getOrCreate()
    lines = spark.read.format("text").load('D:\\PycharmProjects\\Learning1\\filecount.txt')

    wordCounts = lines \
                 .select(explode(split(lines.value, '\s*')).alias("word")) \
                 .groupBy("Word") \
                 .count()
    print("\n Word Count: ",wordCounts.show(20,5))
    spark.stop()