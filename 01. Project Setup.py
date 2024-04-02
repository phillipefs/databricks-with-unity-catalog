# Databricks notebook source
spark.sql(""" USE CATALOG dev_catalog""")

# COMMAND ----------



silver_path = spark.sql("""describe external location `silver`""").select("url").collect()[0][0]
gold_path = spark.sql("""describe external location `gold`""").select("url").collect()[0][0]

# COMMAND ----------

spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `silver` MANAGED LOCATION '{silver_path}/silver' """)

# COMMAND ----------

spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `gold` MANAGED LOCATION '{gold_path}/gold' """)

# COMMAND ----------


