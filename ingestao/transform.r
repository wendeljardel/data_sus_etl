# Databricks notebook source
# DBTITLE 1,Instalação de pacotes
install.packages("read.dbc")
install.packages("doParallel")
install.packages("jsonlite")

# COMMAND ----------

# DBTITLE 1,Setup
library(read.dbc)
library(foreach)
library(doParallel)
library(jsonlite)
library(SparkR)

date = format(Sys.time(), "%Y%m%d")

#datasource <- dbutils.widgets.get("datasource")
datasource <- "sihsus"
datasources <- fromJSON("datasources.json")

path = datasources[datasource][[1]]['target'][[1]]
partes <- unlist(strsplit(path, "/"))
partes <- partes[-length(partes)]
dbc_folder <- paste(partes, collapse = "/")
parquet_folder <- sub('/dbc', '/parquet-1/', dbc_folder)
parquet_folder <- sub('/dbfs', '', parquet_folder)

print(dbc_folder)
print(parquet_folder)

# COMMAND ----------

# DBTITLE 1,Funções
etl <- function(f) {
    df <- createDataFrame(read.dbc(f))
    write.parquet(df, parquet_folder, mode='append')
    file.rename(from=f, to=gsub("landing", "proceeded", f))
    return
}

# COMMAND ----------

# DBTITLE 1,Execução
files <- list.files(dbc_folder, full.names=TRUE)

print("Arquivos a serem processados:")
print(length(files))

# COMMAND ----------

for (i in files){
  print(i)
  etl(i)
}
