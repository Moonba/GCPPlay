#Create Data sets in CSV ! from CloudSQL database morecon to Cloud SQL table
from pyspark.sql import SparkSession


spark = SparkSession \
        .builder \
        .appName("MoreConRecALS") \
        .getOrCreate()


query = "SELECT customer_id, product_id, recommend_level from dtb_review where customer_id!=0"

*****************************************************************************************************************************
SparkSession.sql(sqlQuery)
Returns a DataFrame representing the result of the given query.

Returns:	DataFrame
>>> df.createOrReplaceTempView("table1")
>>> df2 = spark.sql("SELECT field1 AS f1, field2 as f2 from table1")
>>> df2.collect()
[Row(f1=1, f2=u'row1'), Row(f1=2, f2=u'row2'), Row(f1=3, f2=u'row3')]


# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

# The results of SQL queries are Dataframe objects.
sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+
*****************************************************************************************************************************
CONNECT FROM MASTER NODE 

mysql --host=CLOUDSQL_INSTANCE_IP --user=moo --password