from pyspark.sql.functions import *
from pyspark.sql.window import *

import dlt

@dlt.table(
    name='monthly_sales_by_price_tier'
)

def monthly_sales_by_price_tier():
    df_sales=dlt.read('sales_silver')
    df_sales=df_sales.withColumn('month_year',date_format(col('date'),'MM-yyyy'))
    df_price_tier=dlt.read('product_price_tier_gold')
    df_monthly_sales_by_price_tier = df_sales.join(
        df_price_tier,
        df_sales['product_id'] == df_price_tier['product_id'],
        "left"
    )
    df_monthly_sales_by_price_tier=df_monthly_sales_by_price_tier.groupBy('month_year','price_tier')\
        .agg(sum('total_amount').alias('total_revenue'),
            (sum('quantity').alias("total_quantity")),\
            (round(avg('discount'),2).alias('avg_discount')),\
            (count('sales_id').alias('transaction_count')))
    df_monthly_sales_by_price_tier=df_monthly_sales_by_price_tier.withColumn('revenue_per_unit',round(col('total_revenue')/col('total_quantity'),2))
    return df_monthly_sales_by_price_tier