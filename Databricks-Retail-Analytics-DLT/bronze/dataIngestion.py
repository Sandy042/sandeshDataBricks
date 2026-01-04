#Creating a Bronze table from our sourcefiles which are residing in our diffrent volume.
import dlt
from pyspark.sql.functions import *
#CREATE BRONZE STREAMING SALES TABLE
@dlt.table(
    name="sales_bronze"
)

def sales_bronze():
    df_sales=spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", "/Volumes/sandeshmsdatabricks/dlt_schema/dlt_checkpoints/salesCheckPoint/")\
        .option("cloudFiles.schemaEvolutionMode", "rescue")\
        .load("/Volumes/sandeshmsdatabricks/sourcefiles/sourcevolume/DLT_ETL_SOURCE/sales/")
    df_sales=df_sales.withColumn("Processing_DateTime",current_date())
    return df_sales
    
#CREATE BRONZE STREAMING STORES TABLE
@dlt.table(
    name="stores_bronze"
)

def stores_bronze():
    df_store=spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", "/Volumes/sandeshmsdatabricks/dlt_schema/dlt_checkpoints/storesCheckPoint/")\
        .option("cloudFiles.schemaEvolutionMode", "rescue")\
        .load("/Volumes/sandeshmsdatabricks/sourcefiles/sourcevolume/DLT_ETL_SOURCE/stores/")
    df_store=df_store.withColumn("Processing_DateTime",current_date())
    return df_store

#CREATE BRONZE STREAMING PRODUCTS TABLE
@dlt.table(
    name="products_bronze"
)

def products_bronze():
    df_products=spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", "/Volumes/sandeshmsdatabricks/dlt_schema/dlt_checkpoints/productsCheckPoint/")\
        .option("cloudFiles.schemaEvolutionMode", "rescue")\
        .load("/Volumes/sandeshmsdatabricks/sourcefiles/sourcevolume/DLT_ETL_SOURCE/products/")
    df_products=df_products.withColumn("Processing_DateTime",current_date())
    return df_products
    
#CREATE BRONZE STREAMING CUSTOMERS TABLE
@dlt.table(
    name="customers_bronze"
)

def customers_bronze():
    df_customers=spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", "/Volumes/sandeshmsdatabricks/dlt_schema/dlt_checkpoints/customerCheckPoint/")\
        .option("cloudFiles.schemaEvolutionMode", "rescue")\
        .load("/Volumes/sandeshmsdatabricks/sourcefiles/sourcevolume/DLT_ETL_SOURCE/customers/")
    df_customers=df_customers.withColumn("Processing_DateTime",current_date())
    return df_customers
