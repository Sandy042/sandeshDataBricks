from pyspark.sql.functions import *
from pyspark.sql.window import *
import dlt

@dlt.table(
    name='customer_lifetime_value'
)

def customer_lifetime_value():
    df_sales=dlt.read('sales_silver')
    df_customer=dlt.read('customer_silver')
    df_sales=df_sales.withColumn('month_year',date_format(col('date'),'MM-yyyy'))
    df_sales=df_sales.groupBy('customer_id')\
        .agg(
            sum('total_amount').alias('total_spends'),
            sum('quantity').alias('total_quantity_purchased'),
            count('sales_id').alias('total_transations'),
            round(avg('total_amount'),2).alias('avg_order_value'),
            min('date').alias('first_ordered_on'),
            max('date').alias('last_ordered_on')
            )
    df_customer_lifetime_value=df_sales.join(df_customer,'customer_id','left')\
        .select(*df_sales.columns,'firstname','lastname','location',
                datediff(current_date(),col('last_ordered_on')).alias('days_since_last_purchase')
                )
    return df_customer_lifetime_value
    

