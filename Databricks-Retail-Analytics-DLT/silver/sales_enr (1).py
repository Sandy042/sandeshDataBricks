#Here we are creating the silver view and performing the actions,transformation and then loading this data into our silver table.

import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import *

@dlt.table(
    name="sales_silver"
)

def sales_silver():
    df_sales=dlt.readStream("sales_bronze")
    return df_sales.withColumn("sales_id", col("sales_id").cast("int")) \
                   .withColumn("customer_id", col("customer_id").cast("int"))\
                   .withColumn("product_id", col("product_id").cast("int"))\
                   .withColumn("store_id", col("store_id").cast("int"))\
                   .withColumn("quantity", col("quantity").cast("int"))\
                   .withColumn("discount", col("discount").cast("double"))\
                   .withColumn("total_amount", col("total_amount").cast("double"))\
                    .withColumn("date", col("date").cast("date"))
