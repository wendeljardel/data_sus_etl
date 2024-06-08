# Databricks notebook source
# DBTITLE 1,Imports
import sys

sys.path.insert(0, '../lib')

from ingestors import IngestaoBronze
import dbtools
import delta
import json

# COMMAND ----------

# DBTITLE 1,Setup
#table_name=dbutils.widgets.get("datasource")
table_name="sihsus"
database_name='curso_databricks_uc.bronze_datasus.sihsus'

with open("sources.json", "r") as open_file:
    config = json.load(open_file)[table_name]


# COMMAND ----------

file_path = config['path_full_load']
spark.read.format("parquet").load(file_path).write.mode("overwrite").saveAsTable(f'{database_name}.{table_name}')

# COMMAND ----------

spark.read.format("parquet").load(file_path).write.mode("overwrite").save("/dbfs/mnt/datalake/bronze/datasus/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM curso_databricks_uc.bronze_datasus.sihsus;

# COMMAND ----------

sihsus_df = spark.read.table("curso_databricks_uc.bronze_datasus.sihsus")
display(sihsus_df.limit(5))
