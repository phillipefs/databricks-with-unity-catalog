# Databricks notebook source
# MAGIC %md
# MAGIC # To re-use common functions and variables

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Defining all common variables
# MAGIC

# COMMAND ----------

checkpoint = spark.sql("describe external location `checkpoints`").select("url").collect()[0][0]
landing    = spark.sql("describe external location `landing`").select("url").collect()[0][0]
bronze     = spark.sql("describe external location `bronze`").select("url").collect()[0][0]
silver     = spark.sql("describe external location `silver`").select("url").collect()[0][0]
gold       = spark.sql("describe external location `gold`").select("url").collect()[0][0]
