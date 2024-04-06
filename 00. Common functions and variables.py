# Databricks notebook source
# MAGIC %md
# MAGIC # To re-use common functions and variables

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Defining all common variables
# MAGIC

# COMMAND ----------

checkpoint_path = spark.sql("describe external location `checkpoints`").select("url").collect()[0][0]
landing_path    = spark.sql("describe external location `landing`").select("url").collect()[0][0]
bronze_path     = spark.sql("describe external location `bronze`").select("url").collect()[0][0]
silver_path     = spark.sql("describe external location `silver`").select("url").collect()[0][0]
gold_path       = spark.sql("describe external location `gold`").select("url").collect()[0][0]
