
from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()
env=spark.conf.get("spark.databricks_env")

def catalog_name(catalog):
    return catalog+"_"+env

from pyspark.sql.types import StringType
spark.udf.register('catalog_name', catalog_name, StringType())