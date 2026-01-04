from pyspark.sql.functions import *
# from pyspark.sql.Window import *

import dlt

@dlt.table(
    name="product_price_tier_gold"
)
def product_price_tier_gold():
    df_products=dlt.read("products_silver")
    df_products=df_products.withColumn("Price_tier",
                                       when(col("price").between (0,100),'Low Budget')
                                       .when(col("price").between(101,250),'Mid Budget')
                                       .when(col("price").between(250,400),'High Budget')
                                       .when(col("price").between(401,500),'Premium')
                                       .when(col("price") > 500,'Luxury')
                                       )
    return (df_products.select(col("product_id"),col("product_name"),col("category"),col("price"),col("Price_tier")))