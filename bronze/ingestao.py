# Databricks notebook source
# DBTITLE 1,Imports
import sys

sys.path.insert(0, '../lib')

from ingestors import IngestaoBronze
import dbtools
import delta
import json
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Setup
#table_name=dbutils.widgets.get("datasource")
table_name="sihsus"
database_name='curso_databricks_uc.bronze_datasus'

with open("sources.json", "r") as open_file:
    config = json.load(open_file)[table_name]


# COMMAND ----------

file_path = config['path_full_load']
sihsus_df = spark.read.format("parquet").load(file_path)

# COMMAND ----------

# DBTITLE 1,Adicionando Metadados
sihsus_df = sihsus_df.withColumn("data_bronze", F.current_date())


# COMMAND ----------

sihsus_df.write.mode("overwrite").partitionBy('ANO_CMPT').saveAsTable(f'{database_name}.{table_name}')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM curso_databricks_uc.bronze_datasus.sihsus;

# COMMAND ----------

sihsus_df = spark.read.table("curso_databricks_uc.bronze_datasus.sihsus")
display(sihsus_df.limit(15))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(AUD_JUST) FROM curso_databricks_uc.bronze_datasus.sihsus WHERE ANO_CMPT = '2023' AND AUD_JUST LIKE '%ESTRANGEIRO%'
