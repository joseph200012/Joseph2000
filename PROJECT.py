# Databricks notebook source
# DBTITLE 1,Reading the AWS credentials
# File location and type
file_location = "/FileStore/tables/joseph_user_credentials.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
aws_key = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(aws_key)

# COMMAND ----------

# DBTITLE 1,Creating encoded secret key
from pyspark.sql.functions import *
import urllib
ACCESS_KEY = (
    aws_key.where(col("User name") == "Joseph")
    .select("Access key ID")
    .collect()[0]["Access key ID"]
)
SECRET_KEY = (
    aws_key.where(col("User name") == "Joseph")
    .select("Secret access key")
    .collect()[0]["Secret access key"]
)
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# aws bucket name
aws_s3_bucket = "pragathi1692"
# mount name
mount_name = "/mnt/graduation-lab/"
# source url access key,encoded secret key and bucket name
source_url = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, aws_s3_bucket)
dbutils.fs.mount(source_url, mount_name)

# COMMAND ----------

# MAGIC %fs ls '/mnt/graduation-lab/'

# COMMAND ----------

# DBTITLE 1,Reading sales datset
file_location = "/mnt/graduation-lab/sales_data.csv"
file_type = "csv"

infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct order_id from source_sales

# COMMAND ----------

# DBTITLE 1,Correcting the misinterpreted data
df = df.withColumn('Qty_ordered', ( df['qty_ordered'] - 1 ))

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,Dropping the unnecessary columns
df1 = df.drop(*('Phone_No','SSN','Name Prefix','full_name','Middle Initial','E Mail','bi_st','First Name','Last Name','Customer Since','full_name','payment_method'))

# COMMAND ----------

display(df1)

# COMMAND ----------

# DBTITLE 1,Renaming the columns
df2=df1.withColumnRenamed("Place Name","Place_Name") \
      .withColumnRenamed("User Name","User_Name") \
      .withColumnRenamed("sku","Item_Name") \
      .withColumnRenamed("age","Age") \
      .withColumnRenamed("ref_num","Ref_Num") \
      .withColumnRenamed("year","Year") \
      .withColumnRenamed("cust_id","Cust_Id") \
      .withColumnRenamed("category","Category") \
      .withColumnRenamed("total","Total") \
      .withColumnRenamed("status","Status") \
      .withColumnRenamed("month","Month") \
      .withColumnRenamed("discount_amount","Discount_Amount") \
      .withColumnRenamed("value","Value") \
      .withColumnRenamed("price","Price") \
      .withColumnRenamed("Qty_ordered","Qty_Ordered") \
      .withColumnRenamed("item_id","Item_Id") \
      .withColumnRenamed("order_date","Order_Date") \
      .withColumnRenamed("order_id","Order_Id")

# COMMAND ----------

# DBTITLE 1,Filtering the meaningless transactions
df3=df2.filter(df.Qty_ordered != '0')

# COMMAND ----------

# DBTITLE 1,Checking the count of null values
from pyspark.sql.functions import *
col_null_cnt_df =  df3.select([count(when(col(c).isNull(),c)).alias(c) for c in df3.columns])
display(col_null_cnt_df)

# COMMAND ----------

df3.display()

# COMMAND ----------

display(df3.filter(col("Order_Id").isNull()))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *
window = Window.orderBy('Order_Date')
source_df = df3.withColumn('ID', row_number().over(window))


# COMMAND ----------

source_df.display()

# COMMAND ----------

# DBTITLE 1,Source delta table
df3.write.format("delta").saveAsTable("source_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct state from source_sales;

# COMMAND ----------

# DBTITLE 1,Row count check 1 source
# MAGIC %sql
# MAGIC select count(year) from source_sales

# COMMAND ----------

# DBTITLE 1,Group by  row count
# MAGIC %sql
# MAGIC select count(*) from source_sales
# MAGIC group by Gender

# COMMAND ----------

# DBTITLE 1,Stratification testing
# MAGIC %sql
# MAGIC select sum(Total),sum(Discount_Amount),sum(Qty_Ordered) from source_sales

# COMMAND ----------

# DBTITLE 1,Random record check source
# MAGIC %sql
# MAGIC select Item_Name,Qty_Ordered,Category,Total
# MAGIC from source_sales
# MAGIC order by Total desc
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,row count check 2 
# MAGIC %sql 
# MAGIC select year,count(year) from source_sales
# MAGIC group by year
# MAGIC order by year

# COMMAND ----------

# DBTITLE 1,Row count check 3
# MAGIC %sql
# MAGIC select count(State) from source_sales

# COMMAND ----------

# DBTITLE 1,Row count check 4
# MAGIC %sql
# MAGIC select State,count(State)
# MAGIC from source_sales
# MAGIC group by State
# MAGIC order by State

# COMMAND ----------

# DBTITLE 1,Creating staging dataframe
staging_sales=df3

# COMMAND ----------

window = Window.orderBy('order_date')
staging_df1 = staging_sales.withColumn('Sales_ID', row_number().over(window))

# COMMAND ----------

staging_df1.display()

# COMMAND ----------

# DBTITLE 1,Staging dataframe to delta table
staging_df1.write.format("delta").saveAsTable("staging_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from staging_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted staging_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from staging_sales

# COMMAND ----------

# DBTITLE 1,Creating orders dimension table
Orders=staging_sales.select("Order_Id","Ref_Num","Order_Date","Status","Year","Month")

# COMMAND ----------

# DBTITLE 1,Dropping Duplicates
df4 = Orders.dropDuplicates(["Order_Id","Ref_Num","Order_Date","Status","Year","Month"])
display(df4)

# COMMAND ----------

# DBTITLE 1,creating  load, target created date and target modified date
from pyspark.sql.types import DateType
from pyspark.sql.functions import current_date
df4=df4.withColumn("LOAD_DATE", current_date().cast(DateType()))\
 .withColumn("TGT_CREATED_DATE", current_date().cast(DateType()))\
 .withColumn("TGT_MODIFIED_DATE", current_date().cast(DateType()))

# COMMAND ----------

# DBTITLE 1,Creating a unique id column
window = Window.orderBy('Order_Date')
df5 = df4.withColumn('Purchase_Id', row_number().over(window))

# COMMAND ----------

df5.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Order_Id from dim_orders

# COMMAND ----------

# DBTITLE 1,Creating Orders Dimension Delta Table
df5.write.format("delta").saveAsTable("dim_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted dim_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(Month) from dim_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)from dim_orders

# COMMAND ----------

# DBTITLE 1,Creating items dimension table
Items=staging_sales.select("Item_Id","Item_Name","Category")

# COMMAND ----------

# DBTITLE 1,Droping Duplicates
df6 = Items.dropDuplicates(["Item_Id","Item_Name","Category"])
display(df6)

# COMMAND ----------

from pyspark.sql.types import DateType
from pyspark.sql.functions import current_date
df6=df6.withColumn("LOAD_DATE", current_date().cast(DateType()))\
 .withColumn("TGT_CREATED_DATE", current_date().cast(DateType()))\
 .withColumn("TGT_MODIFIED_DATE", current_date().cast(DateType()))

# COMMAND ----------

window = Window.orderBy('Item_Id')
df7 = df6.withColumn('Product_Id', row_number().over(window))

# COMMAND ----------

# DBTITLE 1,Creating Items dimension delta table
df7.write.format("delta").saveAsTable("dim_Items")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted dim_Items

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_Items

# COMMAND ----------

# DBTITLE 1,Creating Customers dimension table
Customers=staging_sales.select("Cust_Id","User_Name","Gender")

# COMMAND ----------

# DBTITLE 1,Dropping Duplicates
df8 = Customers.dropDuplicates(["Cust_Id","User_Name","Gender"])
display(df8)

# COMMAND ----------

from pyspark.sql.types import DateType
from pyspark.sql.functions import current_date
df8=df8.withColumn("LOAD_DATE", current_date().cast(DateType()))\
 .withColumn("TGT_CREATED_DATE", current_date().cast(DateType()))\
 .withColumn("TGT_MODIFIED_DATE", current_date().cast(DateType()))

# COMMAND ----------

# DBTITLE 1,Creating a unique id column
window = Window.orderBy('Cust_Id')
df9 = df8.withColumn('User_Id', row_number().over(window))

# COMMAND ----------

# DBTITLE 1,Creating Customers dimension delta table
df9.write.format("delta").saveAsTable("dim_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted dim_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_customers

# COMMAND ----------

# DBTITLE 1,Creating address dimension table
address=staging_sales.select("State","County","City","Place_Name","Zip")

# COMMAND ----------

# DBTITLE 1,Dropping duplicates
df10 = address.dropDuplicates(["State","County","City","Place_Name","Zip"])
display(df10)

# COMMAND ----------

from pyspark.sql.types import DateType
from pyspark.sql.functions import current_date
df10=df10.withColumn("LOAD_DATE", current_date().cast(DateType()))\
 .withColumn("TGT_CREATED_DATE", current_date().cast(DateType()))\
 .withColumn("TGT_MODIFIED_DATE", current_date().cast(DateType()))

# COMMAND ----------

# DBTITLE 1,Creating a unique id column
window = Window.orderBy('State')
df11 = df10.withColumn('Add_Id', row_number().over(window))

# COMMAND ----------

# DBTITLE 1,Creating address dimension delta table
df11.write.format("delta").saveAsTable("dim_address")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted dim_address

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_address

# COMMAND ----------

# DBTITLE 1,Creating Region dimension table
Region=staging_sales.select("Region")

# COMMAND ----------

# DBTITLE 1,Dropping duplicates
df12 = Region.dropDuplicates(["Region"])
display(df12)

# COMMAND ----------

# DBTITLE 1,Creating load date, target created date and target modified date
from pyspark.sql.types import DateType
from pyspark.sql.functions import current_date
df12=df12.withColumn("LOAD_DATE", current_date().cast(DateType()))\
 .withColumn("TGT_CREATED_DATE", current_date().cast(DateType()))\
 .withColumn("TGT_MODIFIED_DATE", current_date().cast(DateType()))

# COMMAND ----------

# DBTITLE 1,Creating a unique id column
window = Window.orderBy('Region')
df13 = df12.withColumn('Region_Id', row_number().over(window))

# COMMAND ----------

# DBTITLE 1,Creating a region dimension delta table
df13.write.format("delta").saveAsTable("dim_region")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted dim_region

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_region

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from staging_sales

# COMMAND ----------

# DBTITLE 1,Creating fact table
# MAGIC %sql
# MAGIC create table fact_sales
# MAGIC as
# MAGIC select
# MAGIC     stage.Sales_ID,
# MAGIC     dor.Purchase_Id,
# MAGIC     ditm.Product_Id,
# MAGIC     dcus.User_Id,
# MAGIC     dad.Add_Id,
# MAGIC     drg.Region_Id,
# MAGIC     stage.Age,
# MAGIC     stage.Discount_Amount,
# MAGIC     stage.Price,
# MAGIC     stage.Qty_Ordered,
# MAGIC     stage.Total,
# MAGIC     stage.Value,
# MAGIC     stage.Discount_Percent
# MAGIC from
# MAGIC     staging_sales stage
# MAGIC     JOIN dim_orders as dor on stage.Order_Id = dor.Order_Id and stage.Ref_Num = dor.Ref_Num and stage.Order_Date = dor.Order_Date and dor.Status=stage.Status
# MAGIC     and  stage.Year = dor.Year and stage.Month = dor.Month
# MAGIC     JOIN dim_Items as ditm on stage.Item_Id = ditm.Item_Id and stage.Item_Name = ditm.Item_Name and stage.Category = ditm.Category
# MAGIC     JOIN dim_customers as dcus on stage.Cust_Id = dcus.Cust_Id and stage.User_Name = dcus.User_Name and stage.Gender = dcus.Gender
# MAGIC     JOIN dim_address as dad on stage.State = dad.State and stage.County = dad.County and stage.City = dad.City and stage.Place_Name = dad.Place_Name and stage.Zip =     dad.Zip
# MAGIC     JOIN dim_region as drg on stage.Region = drg.Region;
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted fact_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fact_sales

# COMMAND ----------

# DBTITLE 1,Row count check 1 target
# MAGIC %sql
# MAGIC select count(year) from fact_sales join
# MAGIC dim_orders on fact_sales.Purchase_Id=dim_orders.Purchase_Id

# COMMAND ----------

# DBTITLE 1,Row count check 2 target
# MAGIC %sql
# MAGIC select year,count(year) from fact_sales 
# MAGIC join dim_orders on 
# MAGIC fact_sales.Purchase_Id = dim_orders.Purchase_Id
# MAGIC group by year

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from report_sales
# MAGIC group by Gender

# COMMAND ----------

# DBTITLE 1,Stratification test target
# MAGIC %sql
# MAGIC select sum(Total),sum(Discount_Amount),sum(Qty_Ordered) from fact_sales 

# COMMAND ----------

# DBTITLE 1,Random Record Check target
# MAGIC %sql
# MAGIC select Item_Name,Qty_Ordered,Category,Total
# MAGIC from fact_sales join dim_items on fact_sales.Product_Id=dim_items.Product_Id
# MAGIC order by Total desc
# MAGIC limit 10;

# COMMAND ----------

# DBTITLE 1,Row count check 3 target 
# MAGIC %sql
# MAGIC select count(State) from fact_sales join 
# MAGIC dim_address on fact_sales.Add_Id=dim_address.Add_Id

# COMMAND ----------

# DBTITLE 1,Row count check 4 target
# MAGIC %sql
# MAGIC select State,count(State) from fact_sales join
# MAGIC dim_address on fact_sales.Add_Id=dim_address.Add_Id
# MAGIC group by State
# MAGIC order by state

# COMMAND ----------

# DBTITLE 1,Distinct Record Check
# MAGIC %sql
# MAGIC select count(*) from
# MAGIC dim_region
# MAGIC group by region
# MAGIC having count(*)>1

# COMMAND ----------

# DBTITLE 1,Column Level Check
# MAGIC %sql
# MAGIC select
# MAGIC count(*)
# MAGIC from
# MAGIC dim_orders dor
# MAGIC left outer join
# MAGIC (select
# MAGIC fact.Purchase_Id,
# MAGIC dor.Order_Date
# MAGIC from dim_orders as dor
# MAGIC join fact_sales fact on dor.Purchase_Id = fact.Purchase_Id)
# MAGIC as source
# MAGIC on
# MAGIC dor.Purchase_Id = source.Purchase_Id
# MAGIC where
# MAGIC source.Order_Date != dor.Order_Date

# COMMAND ----------

# DBTITLE 1,Sales Report
df_report = spark.sql('''
select dor.Order_Id,
dor.Ref_Num,
dor.Order_Date,
dor.Status,
dor.Year,
dor.Month,
ditm.Item_Id,
ditm.Item_Name,
ditm.Category,
dcus.Cust_Id,
dcus.User_Name,
dcus.Gender,
dad.State,
dad.County,
dad.City,
dad.Place_Name,
dad.Zip,
drg.Region,
fact.Age,
fact.Discount_Amount,
fact.Price,
fact.Qty_Ordered,
fact.Total,
fact.Value,
fact.Discount_Percent
from fact_sales fact
join dim_orders dor on   fact.Purchase_Id = dor.Purchase_Id
join dim_Items ditm on   fact.Product_Id = ditm.Product_Id
join dim_customers dcus on   fact.User_Id = dcus.User_Id
join dim_address dad on   fact.Add_Id = dad.Add_Id
join dim_region drg on   fact.Region_id = drg.Region_id
''')

# COMMAND ----------

display(df_report.display())

# COMMAND ----------

df_report.printSchema()

# COMMAND ----------

df_report.write.format("delta").saveAsTable("Report_Sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Report_sales

# COMMAND ----------


