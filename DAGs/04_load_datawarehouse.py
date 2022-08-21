import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName('dataproc-pyspark') \
        .getOrCreate()
    return spark

def dwh_dimenssions(spark):
    source = spark.read.format("parquet").option("header","true").option('inferSchema','true').load('/FileStore/tables/source/')
    source = source.withColumn("population",col("population").cast("Double"))
    energy = spark.read.format("parquet").option("header","true").option('inferSchema','true').load('/FileStore/tables/parq/')

    # Hive
    energy.createOrReplaceTempView("energytb")
    source.createOrReplaceTempView("sourcetb")
    table_poor = spark.sql("SELECT count(*) FROM sourcetb s join energytb e on s.country = e.country").show()

def main():
    spark = create_spark_session()
    dwh_dimenssions(spark)

if __name__ == "__main__":
    main()