from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType, DecimalType, BooleanType
from pyspark.sql.functions import col,max,row_number,count, mean, desc, lit, trim
from pyspark.sql.window import Window
import sys
from datetime import datetime
def transformData():
    spark = SparkSession.builder \
            .appName('processingData')\
            .getOrCreate()

    print("[1] ------- Spark Session Created")
    bucket = 'your_raw_bucket'
    bucket_master = 'your_master_bucket'
    bq_dataset = 'analytics_dwh'
    pathGCSMaster = "gs://your_master_bucket/master/data/"
    pathGCSPostgres = "gs://your_raw_bucket/raw/data/postgresql/"
    pathGCSOldStorage = "gs://your_raw_bucket/raw/data/old_storage/"

    spark.conf.set('temporaryGcsBucket', bucket)


    customerSchema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("address", StringType(), False),
        StructField("dob", DateType(), False)
                            ])

    customersDebtSchema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("debt", DecimalType(10,2), False)
                                ])

    productsSchema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("price", DecimalType(10,2), False)
                                ])
    storesSchema = StructType([
        StructField("store_id", IntegerType(), False),
        StructField("address", StringType(), True)
                                ])
    transactionsSchema = StructType([
        StructField("transactions_id", IntegerType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("store_id", IntegerType(), False)
                                ])

    cryptoManageSchema = StructType([
        StructField("id", IntegerType(), False),
        StructField("bitcoin_address", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("premium", BooleanType(), False),
        StructField("debt", IntegerType(), False)
                                ])
    print("[2] ------- Create Schema Tables")

    customersDf = spark.read.schema(customerSchema).option("header", True).csv(pathGCSPostgres + "customers.csv")

    customersDebtDf = spark.read.schema(customersDebtSchema).option("header", True).csv(pathGCSOldStorage + "customers_debt.csv")

    productsDf = spark.read.schema(productsSchema).option("header", True).csv(pathGCSPostgres + "products.csv")

    storesDf = spark.read.schema(storesSchema).option("header", True).csv(pathGCSPostgres + "stores.csv")

    transactionsDf = spark.read.schema(transactionsSchema).option("header", True).csv(pathGCSPostgres + "transactions.csv")

    cryptoManageDf = spark.read.schema(cryptoManageSchema).json(pathGCSOldStorage + "crypto_manage.json")
    
    print("[2] ------- Reading files with Schema")

    #Top compras por customer
    customersTransaccion = customersDf.join(
    transactionsDf,transactionsDf.customer_id == customersDf.customer_id,"inner").drop(customersDf.customer_id)

    customerStore = customersTransaccion.join(
        storesDf,storesDf.store_id == customersTransaccion.store_id,"inner")

    customerPrice = customerStore.join(productsDf,productsDf.product_id == customerStore.product_id ).drop(customerStore.product_id)


    windowSpec = Window.partitionBy(col("customer_id")).orderBy(desc("price"))
    rowNumber = row_number().over(windowSpec)
    date_now = datetime.today().strftime('%Y-%m-%d')
    dateCol = lit(date_now).cast(DateType()).alias("ingestion_date")
    topCustomerDF = customerPrice.select(col("customer_id"), col("first_name"),col("last_name"),col("email")
                         , col("price"), rowNumber.alias("top_customer"), dateCol)

    bq_table_customers = 'top_customers'
    topCustomerDF.write.mode('overwrite').option("temporaryGcsBucket", bucket).parquet(pathGCSMaster+"customers/")
    
    topCustomerDF.write \
                   .format("bigquery") \
                   .option("table","{}.{}".format(bq_dataset, bq_table_customers)) \
                   .option("temporaryGcsBucket", bucket) \
                   .mode('overwrite') \
                   .save()
    
    print("[3] ------- Writting top customers table")

    # TopProducts
    topProductsDF = customerPrice.groupBy(col("product_id"),
        col("name"), col("price")).agg(
        count("*").alias("product_count")).select(col("product_id"),col("name"), col("price"), col("product_count")
                                             ).orderBy(desc("product_count"))
    bq_table_top_products = 'top_products'
    topProductsDF.write.mode('overwrite').option("temporaryGcsBucket", bucket).parquet(pathGCSMaster+"products/")
    
    topProductsDF.write \
                   .format("bigquery") \
                   .option("table","{}.{}".format(bq_dataset, bq_table_top_products)) \
                   .option("temporaryGcsBucket", bucket) \
                   .mode('overwrite') \
                   .save()
    
    print("[4] ------- Writting top products table")

    # customers Debt
    cryptoDebt = cryptoManageDf.join(customersDebtDf, customersDebtDf.customer_id == cryptoManageDf.id, "inner").drop(cryptoManageDf.debt)
    customerDebtFree = cryptoDebt.where((col("premium") == True) & (col("debt") > 50.0))



    bq_table_free_debt = 'customers_free_debt'
    customerDebtFree.write.mode('overwrite').option("temporaryGcsBucket", bucket).parquet(pathGCSMaster+"cryptoDebt/")
    
    customerDebtFree.write \
                   .format("bigquery") \
                   .option("table","{}.{}".format(bq_dataset, bq_table_free_debt)) \
                   .option("temporaryGcsBucket", bucket) \
                   .mode('overwrite') \
                   .save()
    
#Call function
transformData()
