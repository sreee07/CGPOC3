from pyspark.sql import SparkSession
from configparser import ConfigParser

def create_spark_session():
    spark = SparkSession.builder.config("spark.jars","C:\Installed_software\Driver\postgresql-42.5.2.jar").appName("jdbc").master("local").getOrCreate()
    return spark


def read_parquet_file(spark, file_path):
    return spark.read.parquet(file_path)

def cust_updated():

    parquet_file_path = "C:/Users/sgollape/PycharmProjects/POC/demo/customers"
    df = spark.read.parquet(parquet_file_path)
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=db_properties["url"], table="customers", properties=db_properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("append").parquet("C:\parquet\dbcust")

def Items_updated():

    parquet_file_path = "C:/Users/sgollape/PycharmProjects/POC/demo/items"
    df = spark.read.parquet(parquet_file_path)
   # df.show()
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=db_properties["url"], table="items" , properties=db_properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("append").parquet("C:\parquet\dbItems")

def order_details():

    parquet_file_path = "C:/Users/sgollape/PycharmProjects/POC/demo/order_details"
    df = spark.read.parquet(parquet_file_path)
  #  df.show()
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=db_properties["url"], table="order_details", properties=db_properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("append").parquet("C:\parquet\dborder_details")

def orders():

    parquet_file_path = "C:/Users/sgollape/PycharmProjects/POC/demo/orders"
    df = spark.read.parquet(parquet_file_path)
    #df.show()
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=db_properties["url"], table="orders", properties=db_properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("append").parquet("C:\parquet\dborders")

def salesperson():

    parquet_file_path = "C:/Users/sgollape/PycharmProjects/POC/demo/salesperson"
    df = spark.read.parquet(parquet_file_path)
   # df.show()
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=db_properties["url"], table="salesperson", properties=db_properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("append").parquet("C:\parquet\dbsalesperson")

def ship_to():

    parquet_file_path = "C:/Users/sgollape/PycharmProjects/POC/demo/ship_to"
    df = spark.read.parquet(parquet_file_path)
    #df.show()
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=db_properties["url"], table="ship_to", properties=db_properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("append").parquet("C:\parquet\dbship_to")

if __name__ == "__main__":

    spark = create_spark_session()
    config = ConfigParser()
    config_path = "C:/Users/sgollape/PycharmProjects/POC/config.properties"
    with open(config_path, "r") as config_file:
        content = config_file.read()
        config.read_string(content)
    db_properties = {
    "driver": config.get("database", "driver"),
    "user": config.get("database", "user"),
    "url": config.get("database", "url"),
    "password": config.get("database", "password")
    }
    cust_updated()
    Items_updated()
    order_details()
    orders()
    salesperson()
    ship_to()

