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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Bronze Tables

# COMMAND ----------

def create_table_raw_traffic(environment):
    print(f'Creating raw_Traffic table in {environment}_catalog')
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{environment}_catalog`.`bronze`.`raw_traffic`
                        (
                            Record_ID INT,
                            Count_point_id INT,
                            Direction_of_travel VARCHAR(255),
                            Year INT,
                            Count_date VARCHAR(255),
                            hour INT,
                            Region_id INT,
                            Region_name VARCHAR(255),
                            Local_authority_name VARCHAR(255),
                            Road_name VARCHAR(255),
                            Road_Category_ID INT,
                            Start_junction_road_name VARCHAR(255),
                            End_junction_road_name VARCHAR(255),
                            Latitude DOUBLE,
                            Longitude DOUBLE,
                            Link_length_km DOUBLE,
                            Pedal_cycles INT,
                            Two_wheeled_motor_vehicles INT,
                            Cars_and_taxis INT,
                            Buses_and_coaches INT,
                            LGV_Type INT,
                            HGV_Type INT,
                            EV_Car INT,
                            EV_Bike INT,
                            Extract_Time TIMESTAMP
                    );""")
    print("************************************")

# COMMAND ----------

def create_table_raw_roads(environment):
    print(f'Creating raw_roads table in {environment}_catalog')
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{environment}_catalog`.`bronze`.`raw_roads`
                        (
                            Road_ID INT,
                            Road_Category_Id INT,
                            Road_Category VARCHAR(255),
                            Region_ID INT,
                            Region_Name VARCHAR(255),
                            Total_Link_Length_Km DOUBLE,
                            Total_Link_Length_Miles DOUBLE,
                            All_Motor_Vehicles DOUBLE
                    );""")
    
    print("************************************")

# COMMAND ----------

create_table_raw_traffic(env)
create_table_raw_roads(env)
