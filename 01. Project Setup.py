# Databricks notebook source
dbutils.widgets.text(name= "env", defaultValue="", label="Environment(LowerCase)")
env = dbutils.widgets.get("env")

# COMMAND ----------


bronze_path = spark.sql("""describe external location `bronze`""").select("url").collect()[0][0]
silver_path = spark.sql("""describe external location `silver`""").select("url").collect()[0][0]
gold_path   = spark.sql("""describe external location `gold`""").select("url").collect()[0][0]

# COMMAND ----------

def create_schema_catalog(environment:str, schema_name:str, path:str):
    print(f"Using {environment}_Catalog")
    spark.sql(f""" USE CATALOG '{environment}_catalog' """)
    
    print(f"Creating {schema_name} Schema in {environment}_Catalog")
    spark.sql(f""" CREATE SCHEMA IF NOT EXISTS `{schema_name}` MANAGED LOCATION '{path}/{schema_name}' """)

# COMMAND ----------

create_schema_catalog(env, "bronze", bronze_path)
create_schema_catalog(env, "silver", silver_path)
create_schema_catalog(env, "gold", gold_path)
