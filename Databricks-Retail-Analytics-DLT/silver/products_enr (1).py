#Here we are creating the silver view and performing the actions,transformation and then loading this data into our silver table.

import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import *

@dlt.table(
    name = 'products_silver'
)
@dlt.expect("valid_price", "price > 0")
@dlt.expect_or_fail("non_null_price", "price IS NOT NULL")

def products_silver():
    df_products=dlt.readStream("products_bronze")
    df_products = df_products.dropDuplicates(["product_name", "category", "Processing_DateTime"])
    return df_products.withColumn("price", col("price").cast("double")) \
                   .withColumn("product_id", col("product_id").cast("int"))

