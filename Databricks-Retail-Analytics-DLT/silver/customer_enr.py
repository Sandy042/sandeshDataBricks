#Here we are creating the silver view and performing the actions,transformation and then loading this data into our silver table.

import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import *

@dlt.table(
    name = 'customer_silver'
)

def customer_silver():
    df_customer=dlt.readStream("customers_bronze")
    df_customer=df_customer.withColumn("splitname",split(col("name"), " "))\
        .withColumn("splitemail",split(col("email"), "@"))
    df_customer=df_customer.withColumn("firstname",element_at(col("splitname"),1))\
    .withColumn("lastname",element_at(col("splitname"),2))\
    .withColumn("domain",element_at(col("splitemail"),2))\
    .withColumn("email",lower(col("email")))
    df_customer=df_customer.drop(col("splitname"),col("splitemail"))
    return df_customer
