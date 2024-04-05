# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Extracting Checkpoint, Bronze, Silver containers URLs
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

checkpoint = spark.sql("describe external location `checkpoints`").select("url").collect()[0][0]

# COMMAND ----------

def write_streaming_table(environment, df_streaming, table_name, schema_table, checkpoint_location, output_mode):

    write_stream = (df_streaming.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint + checkpoint_location)
                .outputMode(output_mode)
                .queryName(table_name)
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`{schema_table}`.`{table_name}`"))
    
    write_stream.awaitTermination()
    print(f'Writing `{environment}_catalog`.`{schema_table}`.`{table_name}` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load Table Silver Traffic Load

# COMMAND ----------

# DBTITLE 1,LOAD TRAFFIC BRONZE
df_bronze_traffic = spark.readStream.table(f"`{env}_catalog`.`bronze`.raw_traffic")

df_bronze_traffic = (df_bronze_traffic
              .dropDuplicates()
              .fillna("Unknown")
              .fillna(0)
              .withColumn('Electric_Vehicles_Count',col('EV_Car') + col('EV_Bike'))
              .withColumn('Motor_Vehicles_Count',col('Electric_Vehicles_Count') + col('Two_wheeled_motor_vehicles') 
                          + col('Cars_and_taxis') + col('Buses_and_coaches') + col('LGV_Type') + col('HGV_Type'))
              .withColumn('transformed_time', current_timestamp())

 )

write_streaming_table(
    environment=env,
    df_streaming=df_bronze_traffic,
    schema_table='silver',
    table_name='silver_traffic',
    checkpoint_location = '/silver_traffic_load/checkpt/',
    output_mode='append'
)

