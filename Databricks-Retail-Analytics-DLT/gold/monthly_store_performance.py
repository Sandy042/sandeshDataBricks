from pyspark.sql.functions import *
from pyspark.sql.window import *

import dlt

@dlt.table(
    name='monthly_store_performance'
)

def monthly_store_performance():
    df_sales=dlt.read('sales_silver')
    df_store=dlt.read('stores_silver')
    df_sales=df_sales.withColumn('month_year',date_format(col('date'),'MM-yyyy'))
    df_sales=df_sales.groupBy('store_id','month_year')\
        .agg(
            sum('total_amount').alias('total_revenue'),
            count('sales_id').alias('total_transactions'),
            round(avg('total_amount'),2).alias('avg_transaction_value'),
            sum('quantity').alias('total_quantity_sold'),
            countDistinct('product_id').alias('unique_products_sold'),
            countDistinct('customer_id').alias('unique_customers')
            )
    df_monthly_store_performance=df_sales.alias('sales').join(df_store,df_sales.store_id==df_store.store_id,'left')
    return (df_monthly_store_performance.select('month_year',col("sales.store_id").alias('store_id'),'store_name','region','total_revenue','total_transactions','avg_transaction_value','total_quantity_sold','unique_products_sold','unique_customers'))