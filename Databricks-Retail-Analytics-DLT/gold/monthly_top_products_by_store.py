from pyspark.sql.functions import *
from pyspark.sql.window import *

import dlt

@dlt.table(
    name='monthly_top_products_by_store'
)


def monthly_top_products_by_store():
    df_sales=dlt.read('sales_silver')
    df_sales=df_sales.withColumn('month_year',date_format(col('date'),'MM-yyyy'))
    df_product=dlt.read('product_price_tier_gold')
    df_store=dlt.read('stores_silver')

    df_sales=df_sales.groupBy('product_id','store_id','month_year')\
        .agg(
            sum('total_amount').alias('product_revenue'),
            sum('quantity').alias('product_quantity'),
            count('sales_id').alias('transactions')
            )
    df_monthly_top_products_by_store=df_sales.join(df_product,'product_id','left').select(*df_sales.columns,col('Price_tier'))

    df_monthly_top_products_by_store=df_monthly_top_products_by_store.join(df_store,"store_id",'left').select(*df_monthly_top_products_by_store.columns,col('store_name'),col('region'))

    window_spec=Window.partitionBy('month_year','region').orderBy(col('product_revenue').desc())
    
    df_monthly_top_products_by_store=df_monthly_top_products_by_store.withColumn('Rank',rank().over(window_spec))

    return df_monthly_top_products_by_store
    
