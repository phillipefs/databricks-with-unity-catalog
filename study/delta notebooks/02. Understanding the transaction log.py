# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.mathlab.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.mathlab.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.mathlab.dfs.core.windows.net", f"{client_id_app}")
spark.conf.set("fs.azure.account.oauth2.client.secret.mathlab.dfs.core.windows.net", f"{client_secret_app}")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.mathlab.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id_app}/oauth2/token")

# COMMAND ----------

source = 'abfss://test@deltadbstg.dfs.core.windows.net/'

# COMMAND ----------

dbutils.fs.ls(f'{source}/delta/')

# COMMAND ----------

dbutils.fs.ls(f'{source}/delta/_delta_log')

# COMMAND ----------

display(spark.read.format('text').load('abfss://test@deltadbstg.dfs.core.windows.net/delta/_delta_log/00000000000000000000.json'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading the delta lake file

# COMMAND ----------

df = (spark.read.format('delta')
                .load(f'{source}/delta/'))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df_delta = df.filter("Education_Level =='High School'")

# COMMAND ----------

df_delta.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Overwriting the same file in delta folder

# COMMAND ----------

(df_delta.write.format('delta')
        .mode('overwrite')
        .save(f'{source}/delta/'))

# COMMAND ----------

display(spark.read.format('text').load('abfss://test@deltadbstg.dfs.core.windows.net/delta/_delta_log/00000000000000000001.json'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading the overwritten file

# COMMAND ----------

df_overwrite = (spark.read.format('delta')
                .load(f'{source}/delta/'))

# COMMAND ----------

display(df_overwrite)
