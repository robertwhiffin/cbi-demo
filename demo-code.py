# Databricks notebook source
from utils import *

# COMMAND ----------

catalogues = spark.sql("show catalogs").collect()
for c in catalogues:
    if "cbi" in c.catalog and env in c.catalog:
        spark.sql(f"drop catalog {c.catalog} cascade")

# COMMAND ----------

catalogues = ["cbi_demo", "cbi_fraud", "cbi_money"]
catalogues = list(map(catalog_name, catalogues))
schemas = {
      "cbi_demo" : ["customers", "payments"]
    , "cbi_fraud": ["perpetrators", "victims"]
    , "cbi_money": ["ireland", "UK"]
}
for k in schemas.keys():
    schemas[catalog_name(k)] = schemas.pop(k)
tables = [f"table{x}" for x in [1,2,3]]

# COMMAND ----------

for catalog in catalogues:
    spark.sql(f"create catalog {catalog} ")
    for schema in schemas[catalog]:
        spark.sql(f"create schema {catalog}.{schema}")
        cs = f"{catalog}.{schema}."
        for table in tables:
            spark.sql(f"create table {cs}{table} as select 1 as value")

# COMMAND ----------

spark.sql(f"""
select * from {catalog_name("cbi_money")}.ireland.table1
""").display()

# COMMAND ----------

spark.read.table(f"{catalog_name('cbi_money')}.ireland.table1").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cbi_money_${spark.databricks_env}.ireland.table1
