from pyspark.sql.functions import udf
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import month
from spark.sql.functions import row_number
from pyspark.sql.functions import rank, min
from pyspark.sql.window import Window

#Configure the Product, Sales, Refund & Customer dataframes

dfProduct = spark.read.option("delimiter", "|").format("com.databricks.spark.csv").load("/Users/sujeet/Desktop/codingTest/data/Product.txt").toDF("product_id", "product_name", "product_type", "product_version", "product_price")


#Configure the Product, Sales, Refund & Customer dataframes
dfSales = spark.read.option("delimiter", "|").format("com.databricks.spark.csv").load("/Users/sujeet/Desktop/codingTest/data/Sales.txt").toDF("transaction_id", "customer_id", "product_id", "timestamp", "total_amount", "total_quantity")


#Configure the Product, Sales, Refund & Customer dataframes
dfRefund = spark.read.option("delimiter", "|").format("com.databricks.spark.csv").load("/Users/sujeet/Desktop/codingTest/data/Refund.txt").toDF("refund_id", "original_transaction_id", "customer_id", "product_id", "timestamp", "refund_amount",
"refund_quantity").select('refund_id', 'original_transaction_id', 'customer_id', 'product_id', 'refund_amount', 'refund_quantity', 'timestamp', from_unixtime(unix_timestamp('timestamp', 'MM/dd/yyyy')).alias('Date')).withColumn("month", month('Date'))

#Configure the Product, Sales, Refund & Customer dataframes
dfCustomer = spark.read.option("delimiter", "|").format("com.databricks.spark.csv").load("/Users/sujeet/Desktop/codingTest/data/Customer.txt").toDF("customer_id", "customer_first_name", "customer_last_name", "phone_number")

# Create table or view on the data frames
dfProduct.createOrReplaceTempView("Product")
dfSales.createOrReplaceTempView("Sales")
dfRefund.createOrReplaceTempView("Refund")
dfCustomer.createOrReplaceTempView("Customer")

# Transformations.Register a function to strip '$'.
sqlContext.registerFunction("charReplace", lambda x: x.replace(u'$','') if x else None)

# 2.) Display the distribution of sales by product name and product type self.
dfSalesDistribution = spark.sql("select p.product_id,p.product_name, p.product_type, sum(cast(charReplace(total_amount) as int)) as sales  from Product p left join Sales s on p.product_id = s.product_id group by p.product_name, p.product_type, p.product_id")
dfSalesDistribution.show()

# 3.) Calculate the total amount of all transactions that happened in year 2013 and have not been
#refunded as of today.
dfTransactionNoRefund = spark.sql("select sum(cast(charReplace(total_amount) as int)) as sales from Sales s left join Refund r on s.transaction_id = r.original_transaction_id and r.refund_id is null where to_date(cast(unix_timestamp(s.timestamp, 'MM/dd/yyyy') as TIMESTAMP)) > '2012-12-31' and to_date(cast(unix_timestamp(s.timestamp, 'MM/dd/yyyy') as TIMESTAMP)) < '2014-01-01'")
dfTransactionNoRefund.show()


# 4.) Display the customer name who made the second most purchases in the month of May 2013.
#Refunds should be excluded
NetPurchases = spark.sql("select c.customer_id, c.customer_first_name, c.customer_last_name,  sum(cast(charReplace(s.total_amount) as int)) - sum(cast(charReplace(r.refund_amount) as int)) as netSales, to_date(cast(unix_timestamp(s.timestamp, 'MM/dd/yyyy') as TIMESTAMP)) as date from Customer c left join Sales s on c.customer_id = s.customer_id join Refund r on r.original_transaction_id = s.transaction_id  where to_date(cast(unix_timestamp(s.timestamp, 'MM/dd/yyyy') as TIMESTAMP)) >='2013-05-01' and to_date(cast(unix_timestamp(s.timestamp, 'MM/dd/yyyy') as TIMESTAMP)) <'2013-06-01' group by c.customer_id, c.customer_first_name, c.customer_last_name, to_date(cast(unix_timestamp(s.timestamp, 'MM/dd/yyyy') as TIMESTAMP))").withColumn("month", month('date'))

w = Window.partitionBy("month").orderBy(NetPurchases.netSales.desc())
data = NetPurchases.withColumn("rank", dense_rank().over(w))
result = data.where(data["rank"] == 2)
result.show()


# 5.) Find a product that has not been sold at least once (if any)
# Ans: There is no product that has not been sold at least once.
dfProductNotSold = spark.sql("select * from Product where product_id not in (select distinct product_id from Sales)")
dfProductNotSold.show()

# 6.) Calculate the total number of users who purchased the same product consecutively at least 2 times on a given day.
dfConsecutiveUsers = spark.sql("select count(customer_id) as count, product_id, to_date(cast(unix_timestamp(timestamp, 'MM/dd/yyyy') as TIMESTAMP)) as date from Sales group by product_id, to_date(cast(unix_timestamp(timestamp, 'MM/dd/yyyy') as TIMESTAMP)) having count(customer_id)>= 2").groupBy().sum("count")
dfConsecutiveUsers.show()
