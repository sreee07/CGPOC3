from pyspark.sql import SparkSession
from configparser import ConfigParser
from pyspark.sql.functions import current_date
#from pyspark.sql.functions import weekofyear, month

def create_spark_session():
    spark = SparkSession.builder.config("spark.jars","C:\Installed_software\Driver\postgresql-42.5.2.jar").appName("jdbc").master("local").getOrCreate()
    return spark

#1st report
def generate_monthly_report(spark,input_path_cust,input_path_orders):
    # Read the Parquet file
    df = spark.read.parquet(input_path_cust)
    df1 = spark.read.parquet(input_path_orders)
    #df.show()
    df.createOrReplaceTempView("customers2")
    df1.createOrReplaceTempView("orders2")
    query = "select c.cust_id,c.cust_name,count(order_id) as order_count from customers2 c,orders2 r where c.cust_id=r.cust_id group by c.cust_id,c.cust_name"

    result_df = spark.sql(query)
    df_with_date = result_df.withColumn("current_date", current_date())
    df_with_date.show()
    table_name = "query1_Monthly_weekly_customerWise_order_count"
    df_with_date.write \
    .mode("overwrite") \
    .jdbc(db_properties["url"], table_name, properties=db_properties)
    df_with_date.write.mode("overwrite").partitionBy("current_date").parquet("C:\Parquet\Localpath2\query1_Monthly_weekly_customerWise_order_count")

#2nd report
def generate_monthly_sum_report(spark,input_path_cust,input_path_orders):
    # Read the Parquet file
    df = spark.read.parquet(input_path_cust)
    df1 = spark.read.parquet(input_path_orders)
    #df.show()
    df.createOrReplaceTempView("customers2")
    df1.createOrReplaceTempView("orders2")
    query = "select c.cust_id,c.cust_name,sum(order_id) as sum_count from customers2 c,orders2 r where c.cust_id=r.cust_id group by c.cust_id,c.cust_name"

    result_df = spark.sql(query)
    df_with_date = result_df.withColumn("current_date", current_date())
    df_with_date.show()
    table_name = "query2_Monthly_weekly_customerWise_Sum_order_count"
    df_with_date.write \
    .mode("overwrite") \
    .jdbc(db_properties["url"], table_name, properties=db_properties)
    df_with_date.write.mode("overwrite").partitionBy("current_date").parquet("C:\Parquet\Localpath2\query2_Monthly_weekly_customerWise_Sum_order_count")

#3rd report
def generate_itemWise_total_order_count(spark, input_path_details, input_path_items):
    # Read the Parquet file
    df = spark.read.parquet(input_path_details)
    df1 = spark.read.parquet(input_path_items)
    #df.show()
    df.createOrReplaceTempView("order_details2")
    df1.createOrReplaceTempView("items2")
    query2 = "select i.item_id,i.item_description,od.item_quantity as Sum from items2 i,order_details2 od where i.item_id=od.item_id order by Sum desc"

    result_df = spark.sql(query2)
    df_with_date2 = result_df.withColumn("current_date", current_date())
    df_with_date2.show()
    table_name = "query3_Item_Wise_Total_Order_Count"
    df_with_date2.write \
    .mode("overwrite") \
    .jdbc(db_properties["url"], table_name, properties=db_properties)
    df_with_date2.write.mode("overwrite").partitionBy("current_date").parquet("C:\Parquet\Localpath2\query3_Item_Wise_Total_Order_Count")

#4th report
def generate_ItemName_categoryWise_total_order_count(spark,input_path_items,input_path_details):
    # Read the Parquet file
    df = spark.read.parquet(input_path_items)
    df1 = spark.read.parquet(input_path_details)
    #df.show()
    df.createOrReplaceTempView("items2")
    df1.createOrReplaceTempView("order_details2")
    query = "select i.category ,sum(od.item_quantity * od.detail_unit_price) as Sum from items2 i left join order_details2 od on (od.item_id = i.item_id) group by i.category  order by Sum desc"
    result_df = spark.sql(query)
    df_with_date = result_df.withColumn("current_date", current_date())
    df_with_date.show()
    table_name = "query4_Item_Name_Category_Total_order_count_desc"
    df_with_date.write \
    .mode("overwrite") \
    .jdbc(db_properties["url"], table_name, properties=db_properties)
    df_with_date.write.mode("overwrite").partitionBy("current_date").parquet("C:\Parquet\Localpath2\query4_Item_Name_Category_Total_order_count_desc")

#5th report
def generate_itemWise_total_order_amount(spark,input_path_items,input_path_details):
    # Read the Parquet file
    df = spark.read.parquet(input_path_items)
    df1 = spark.read.parquet(input_path_details)
    #df.show()
    df.createOrReplaceTempView("items2")
    df1.createOrReplaceTempView("order_details2")
    query = "select i.item_id,i.item_description,sum(od.item_quantity) as Sum from items2 i left join order_details2 od on (od.item_id = i.item_id) group by i.item_id,i.item_description  order by Sum desc"

    result_df = spark.sql(query)
    df_with_date = result_df.withColumn("current_date", current_date())
    df_with_date.show()
    table_name = "query5_itemWise_totalOrder_amount"
    df_with_date.write \
    .mode("overwrite") \
    .jdbc(db_properties["url"], table_name, properties=db_properties)
    df_with_date.write.mode("overwrite").partitionBy("current_date").parquet("C:\Parquet\Localpath2\query5_itemWise_totalOrder_amount")

#6th report
def generate_ItemName_categoryWise_total_order_amount(spark,input_path_items,input_path_details):
    # Read the Parquet file
    df = spark.read.parquet(input_path_items)
    df1 = spark.read.parquet(input_path_details)
    #df.show()
    df.createOrReplaceTempView("items2")
    df1.createOrReplaceTempView("order_details2")
    query = "select i.CATEGORY , sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) as Sum from items2 i left join order_details2 od on (od.ITEM_ID = i.ITEM_ID) group by i.CATEGORY  order by Sum desc"

    result_df = spark.sql(query)
    df_with_date = result_df.withColumn("current_date", current_date())
    df_with_date.show()
    table_name = "query6_ItemName_Category_TotalOrder_Amount"
    df_with_date.write \
    .mode("overwrite") \
    .jdbc(db_properties["url"], table_name, properties=db_properties)
    df_with_date.write.mode("overwrite").partitionBy("current_date").parquet("C:\Parquet\Localpath2\query6_ItemName_Category_TotalOrder_Amount")

#7th report
def generate_salesmanWise_total_order_amount(spark, input_path_cust,input_path_details,input_path_orders,input_path_sales):
    # Read the Parquet file
    df = spark.read.parquet(input_path_cust)
    df1 = spark.read.parquet(input_path_details)
    df2 = spark.read.parquet(input_path_orders)
    df3 = spark.read.parquet(input_path_sales)
    #df.show()
    df.createOrReplaceTempView("customers2")
    df1.createOrReplaceTempView("order_details2")
    df2.createOrReplaceTempView("orders2")
    df3.createOrReplaceTempView("salesperson2")
    query = "select s.salesman_id,sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) total_ord_amt,round(sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) * 0.1,2) commission from salesperson2 s  left join customers2 c on (s.salesman_id = c.salesman_id) left join orders2 rd on (c.cust_id= rd.cust_id ) left join order_details2 od on ( rd.order_id = od.order_id) group by s.salesman_id"

    result_df = spark.sql(query)
    df_with_date = result_df.withColumn("current_date", current_date())
    df_with_date.show()
    table_name = "query7_salesman_totalOrderAmount_salesmanIncentive_PreviousMonth"
    df_with_date.write \
    .mode("overwrite") \
    .jdbc(db_properties["url"], table_name, properties=db_properties)
    df_with_date.write.mode("overwrite").partitionBy("current_date").parquet("C:\Parquet\Localpath2\query7_salesman_totalOrderAmount_salesmanIncentive_PreviousMonth")

#8th report
def generate_reports_itemsNotSold_previousMonth(spark, input_path_items,input_path_details):
    # Read the Parquet file
    df = spark.read.parquet(input_path_items)
    df1 = spark.read.parquet(input_path_details)
    #df.show()
    df.createOrReplaceTempView("items2")
    df1.createOrReplaceTempView("order_details2")
    query = "select i.item_id,i.item_description from items2 i where i.item_id not in (select item_id from order_details2 od )"

    result_df = spark.sql(query)
    df_with_date = result_df.withColumn("current_date", current_date())
    df_with_date.show()
    table_name = "query8_reports_itemsNotSold_previousMonth"
    df_with_date.write \
    .mode("overwrite") \
    .jdbc(db_properties["url"], table_name, properties=db_properties)
    df_with_date.write.mode("overwrite").partitionBy("current_date").parquet("C:\Parquet\Localpath2\query8_reports_itemsNotSold_previousMonth")

#9th report
def generate_reports_customers_ordersShipped_previousMonth(spark, input_path_cust,input_path_shipto):
    # Read the Parquet file
    df = spark.read.parquet(input_path_cust)
    df1 = spark.read.parquet(input_path_shipto)
    #df.show()
    df.createOrReplaceTempView("customers2")
    df1.createOrReplaceTempView("ship_to2")
    query = "select c.cust_id, c.cust_name from customers2 c where c.cust_id not in (select s.cust_id  from ship_to2 s)"

    result_df = spark.sql(query)
    df_with_date = result_df.withColumn("current_date", current_date())
    df_with_date.show()
    table_name = "query9_reports_customers_ordersShipped_previousMonth"
    df_with_date.write \
    .mode("overwrite") \
    .jdbc(db_properties["url"], table_name, properties=db_properties)
    df_with_date.write.mode("overwrite").partitionBy("current_date").parquet("C:\Parquet\Localpath2\query9_reports_customers_ordersShipped_previousMonth")

#10th report
def generate_customer_shipment_address_notFilled_correctly(spark, input_path_cust,input_path_shipto):
    # Read the Parquet file
    df = spark.read.parquet(input_path_cust)
    df1 = spark.read.parquet(input_path_shipto)
    #df.show()
    df.createOrReplaceTempView("customers2")
    df1.createOrReplaceTempView("ship_to2")
    query = "select c.cust_id, c.cust_name from customers2 c join ship_to2 s  on (s.cust_id =c.cust_id)  where length(s.postal_code) !=6 "

    result_df = spark.sql(query)
    df_with_date = result_df.withColumn("current_date", current_date())
    df_with_date.show()
    table_name = "query10_customer_shipment_address_notFilled_correctly"
    df_with_date.write \
    .mode("overwrite") \
    .jdbc(db_properties["url"], table_name, properties=db_properties)
    df_with_date.write.mode("overwrite").partitionBy("current_date").parquet("C:\Parquet\Localpath2\query10_customer_shipment_address_notFilled_correctly")

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
    # Define your input paths
    #Customer
    input_path_cust = "C:/Users/sgollape/PycharmProjects/POC/demo/customers/part-00000-66418552-e6e6-485f-aca9-b19cf323f164-c000.snappy.parquet"
    #items
    input_path_items = "C:/Users/sgollape/PycharmProjects/POC/demo/items/part-00000-d194c760-96a3-402e-b109-615a26978f04-c000.snappy.parquet"
    #order_details
    input_path_details = "C:/Users/sgollape/PycharmProjects/POC/demo/order_details/part-00000-a9c0a74c-812f-480f-a794-6f139e40bc44-c000.snappy.parquet"
    #orders
    input_path_orders = "C:/Users/sgollape/PycharmProjects/POC/demo/orders/part-00000-494e7ab9-90cd-4b3c-af03-400fa5473065-c000.snappy.parquet"
    #salesperson
    input_path_sales = "C:/Users/sgollape/PycharmProjects/POC/demo/salesperson/part-00000-435865c1-e67d-426c-9854-17cd94fb8577-c000.snappy.parquet"
    #ship_to
    input_path_shipto = "C:/Users/sgollape/PycharmProjects/POC/demo/ship_to/part-00000-64d4f1d3-7b44-4d80-8993-33ae78812936-c000.snappy.parquet"

    # Call the generate_monthly_report method for each report
    generate_monthly_report(spark,input_path_cust,input_path_orders)#1
    generate_monthly_sum_report(spark,input_path_cust,input_path_orders)#2
    generate_itemWise_total_order_count(spark, input_path_details, input_path_items)#3
    generate_ItemName_categoryWise_total_order_count(spark,input_path_items,input_path_details)#4
    generate_itemWise_total_order_amount(spark,input_path_items,input_path_details)#5
    generate_ItemName_categoryWise_total_order_amount(spark,input_path_items,input_path_details)#6
    generate_salesmanWise_total_order_amount(spark, input_path_cust,input_path_details,input_path_orders,input_path_sales)#7
    generate_reports_itemsNotSold_previousMonth(spark, input_path_items,input_path_details)#8
    generate_reports_customers_ordersShipped_previousMonth(spark, input_path_cust,input_path_shipto)#9
    generate_customer_shipment_address_notFilled_correctly(spark, input_path_cust,input_path_shipto)#10
    # Stop the Spark session

    spark.stop()
# hai
 #hello
# how are you
