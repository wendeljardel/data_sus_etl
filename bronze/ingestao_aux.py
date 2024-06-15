# Databricks notebook source
# DBTITLE 1,Ingestão
def ingest_table(path):
    
    table_name = path.split("/")[-1].replace(".csv", "").lower()
    table_name

    database_table = f"curso_databricks_uc.bronze_datasus.{table_name}"
    
    

    df = (spark.read
            .csv(path, sep=',', header=False)
            .dropna(how="any"))

    (df.write
       .mode("overwrite")
       .format("delta")
       .option("overwriteSchema", "true")
       .saveAsTable(database_table))

files = [i.path for i in dbutils.fs.ls("/mnt/datalake/raw/datasus/aux/")]

for f in files:
    print(f)
    ingest_table(f)
