#Here we are creating the silver view and performing the actions,transformation and then loading this data into our silver table.

import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import *

@dlt.table(
    name="stores_silver"
)

def stores_silver():
    df_stores=dlt.readStream("stores_bronze")
    return df_stores.withColumn("store_id", col("store_id").cast("int"))
