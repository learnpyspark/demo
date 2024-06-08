from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName('test_app') \
        .getOrCreate()
print(spark)
test_df = spark.read.option('header', 'true') \
                    .option('inferSchema', 'true') \
                    .format("csv") \
                    .load("D:\\Downloads\\test1.csv")

test_df.printSchema()
test_df = test_df.withColumn("AvgSalary", (col("Salary")/col("Experience")).cast("Integer"))
##test_df = test_df.withColumn('Gender', lit('male'))
test_df.show()
