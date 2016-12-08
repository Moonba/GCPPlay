spark-submit \
--driver-class-path mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar \
--jars mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar \
/home/mouna_balghouthi/cleandata.py \
My_Instance_IP \
DbName \
user \
pass

#Create Data sets in CSV ! from CloudSQL database morecon to Cloud SQL table
import sys
import itertools
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType


spark = SparkSession \
        .builder \
        .appName("MoreConRecALS") \
        .getOrCreate()


CLOUDSQL_INSTANCE_IP = sys.argv[1] 
CLOUDSQL_DB_NAME = sys.argv[2]
CLOUDSQL_USER = sys.argv[3] 
CLOUDSQL_PWD  = sys.argv[4] 


jdbcUrl = 'jdbc:mysql://%s:3306/%s' % (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME)

dfRatings = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", 'morecon.dtb_review') \
    .option("user", CLOUDSQL_USER) \
    .option("password", CLOUDSQL_PWD) \
    .load()

dfRatings = dfRatings.select(dfRatings['customer_id'], dfRatings['product_id'], dfRatings['recommend_level']).filter(dfRatings['customer_id'] != 0).show()

dfRatings.show()

dfRatings.printSchema()

# Save to cleaned Tables

dfRatings.write \
    .jdbc(jdbcUrl, "morecon.dtb_ratings",
            properties={"user": CLOUDSQL_USER, "password": CLOUDSQL_PWD})

# #[END save_top]

