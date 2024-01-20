# Databricks notebook source
storage_name      = dbutils.secrets.get(scope="kv-databricks", key="storage-name")
client_id_app     = dbutils.secrets.get(scope="kv-databricks", key="client-id-databricks")
tenent_id_app     = dbutils.secrets.get(scope="kv-databricks", key="tenent-id-databricks")
client_secret_app = dbutils.secrets.get(scope="kv-databricks", key="secret-databricks")

# COMMAND ----------

# DBTITLE 1,Configs App Registration
spark.conf.set("fs.azure.account.auth.type.mathlab.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.mathlab.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.mathlab.dfs.core.windows.net", f"{client_id_app}")
spark.conf.set("fs.azure.account.oauth2.client.secret.mathlab.dfs.core.windows.net", f"{client_secret_app}")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.mathlab.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id_app}/oauth2/token")

# COMMAND ----------

source = 'abfss://layer-bronze@mathlab.dfs.core.windows.net/databricks-uc-course/sample/'



# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,FloatType,DoubleType

schema1 = StructType([
    StructField('Education_Level',StringType()),
    StructField('Line_Number',IntegerType()),
    StructField('Employed',IntegerType()),
    StructField('Unemployed',IntegerType()),
    StructField('Industry',StringType()),
    StructField('Gender',StringType()),
    StructField('Date_Inserted',StringType()),
    StructField('dense_rank',IntegerType())
])

# COMMAND ----------

df = (spark.read.format('csv')
            .option('header','true')
            .schema(schema1)
            .load(f'{source}/*.csv'))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Writing to parquet format

# COMMAND ----------

(df.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/ParquetFolder/'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading parquet File

# COMMAND ----------

df_parquet = (spark.read.format('parquet')
            .load(f'{source}/ParquetFolder/'))

# COMMAND ----------

df_parquet.printSchema()

# COMMAND ----------

df_parquet.createOrReplaceTempView('ParquetView')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE ParquetView 
# MAGIC SET Education_Level = 'School'
# MAGIC WHERE Education_Level = 'High School'

# COMMAND ----------

spark.sql("""   UPDATE ParquetView SET Education_Level = 'School' WHERE Education_Level = 'High School'  """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Filter and overwrite 

# COMMAND ----------

df_parquet = df_parquet.filter("Education_level == 'High School'")

# COMMAND ----------

(df_parquet.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/Temp/'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading parquet file from Temp folder

# COMMAND ----------

df_temp = (spark.read.format('parquet')
                    .load(f'{source}/Temp/'))

# COMMAND ----------

display(df_temp)

# COMMAND ----------

(df_temp.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/ParquetFolder/'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading the overwritten file

# COMMAND ----------

df_parquet_ov = (spark.read.format('parquet')
            .load(f'{source}/ParquetFolder/'))

# COMMAND ----------

display(df_parquet_ov)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating delta lake
# MAGIC

# COMMAND ----------

(df.write.format('delta')
    .mode('overwrite')
    .save(f'{source}/delta/'))
