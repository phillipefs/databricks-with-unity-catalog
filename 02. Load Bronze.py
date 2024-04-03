# Databricks notebook source
dbutils.widgets.text(name= "env", defaultValue="", label="Environment(LowerCase)")
env = dbutils.widgets.get("env")

# COMMAND ----------

checkpoint = spark.sql("describe external location `checkpoints`").select("url").collect()[0][0]
landing = spark.sql("describe external location `landing`").select("url").collect()[0][0]

# COMMAND ----------

print(checkpoint)

# COMMAND ----------

def read_traffic_data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading the Raw Traffic Data :  ", end='')
    schema = StructType([
    StructField("Record_ID",IntegerType()),
    StructField("Count_point_id",IntegerType()),
    StructField("Direction_of_travel",StringType()),
    StructField("Year",IntegerType()),
    StructField("Count_date",StringType()),
    StructField("hour",IntegerType()),
    StructField("Region_id",IntegerType()),
    StructField("Region_name",StringType()),
    StructField("Local_authority_name",StringType()),
    StructField("Road_name",StringType()),
    StructField("Road_Category_ID",IntegerType()),
    StructField("Start_junction_road_name",StringType()),
    StructField("End_junction_road_name",StringType()),
    StructField("Latitude",DoubleType()),
    StructField("Longitude",DoubleType()),
    StructField("Link_length_km",DoubleType()),
    StructField("Pedal_cycles",IntegerType()),
    StructField("Two_wheeled_motor_vehicles",IntegerType()),
    StructField("Cars_and_taxis",IntegerType()),
    StructField("Buses_and_coaches",IntegerType()),
    StructField("LGV_Type",IntegerType()),
    StructField("HGV_Type",IntegerType()),
    StructField("EV_Car",IntegerType()),
    StructField("EV_Bike",IntegerType())
    ])

    df_raw_traffic_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{checkpoint}/raw_traffic_load/schema_infer')
        .option('header','true')
        .schema(schema)
        .load(landing+'/raw_traffic/')
        .withColumn("Extract_Time", current_timestamp()))
    
    print('Reading Succcess !!')
    print('*******************')

    return df_raw_traffic_stream

# COMMAND ----------

def read_road_data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading the Raw Roads Data :  ", end='')
    schema = StructType([
        StructField('Road_ID',IntegerType()),
        StructField('Road_Category_Id',IntegerType()),
        StructField('Road_Category',StringType()),
        StructField('Region_ID',IntegerType()),
        StructField('Region_Name',StringType()),
        StructField('Total_Link_Length_Km',DoubleType()),
        StructField('Total_Link_Length_Miles',DoubleType()),
        StructField('All_Motor_Vehicles',DoubleType())
        
        ])

    rawRoads_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{checkpoint}/raw_road_load/schema_infer')
        .option('header','true')
        .schema(schema)
        .load(landing+'/raw_roads/')
        )
    
    print('Reading Succcess !!')
    print('*******************')

    return rawRoads_stream
