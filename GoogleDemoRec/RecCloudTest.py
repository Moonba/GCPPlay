#Run from Master Node where is located the driverFile 
#spark-submit --driver-class-path mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar --jars mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar /home/rectest4.py IP DB user pass

#!/usr/bin/env python
import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType

spark = SparkSession \
        .builder \
        .appName("MoreConRecALS") \
        .getOrCreate()

USER_ID = 312940
CLOUDSQL_INSTANCE_IP = sys.argv[1] 
CLOUDSQL_DB_NAME = sys.argv[2] 
CLOUDSQL_USER = sys.argv[3]
CLOUDSQL_PWD  = sys.argv[4]

BEST_RANK = 20
BEST_ITERATION = 10
BEST_REGULATION = 0.1

TABLE_ITEMS  = "dtb_products"
TABLE_RATINGS = "dtb_ratings"
TABLE_RECOMMENDATIONS = "dtb_recommendations"

# Read the data from the Cloud SQL
# Create dataframes
#[START read_from_sql]
# jdbcDriver = 'com.mysql.jdbc.Driver'
jdbcUrl = 'jdbc:mysql://%s:3306/%s' % (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME)

dfRates = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", 'morecon.dtb_ratings') \
    .option("user", CLOUDSQL_USER) \
    .option("password", CLOUDSQL_PWD) \
    .load()

dfProducts = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", 'morecon.dtb_products') \
    .option("user", CLOUDSQL_USER) \
    .option("password", CLOUDSQL_PWD) \
    .load()

#[END read_from_sql]

# Get all the ratings rows of our customer

dfUserRatings  = dfRates.filter(dfRates.customer_id == USER_ID).rdd.map(lambda r: r.product_id).collect()

# Returns only the products that have not been rated by our customer
rddPotential  = dfProducts.rdd.filter(lambda x: x[0] not in dfUserRatings)
pairsPotential = rddPotential.map(lambda x: (USER_ID, x[0]))
#[START split_sets]
rddTraining, rddValidating, rddTesting = dfRates.rdd.randomSplit([6,2,2])
#[END split_sets]

#[START predict]
# Build our model with the best found values
# Rating, Rank, Iteration, Regulation
model = ALS.train(rddTraining, 20, 10, 0.1)
# Calculate all predictions
predictions = model.predictAll(pairsPotential).map(lambda p: (p[0], p[1], float(p[2])))

# Take the top 5 ones
topPredictions = predictions.takeOrdered(5, key=lambda x: -x[2])
print(topPredictions)

#[START save_top]
schema = StructType([StructField("customer_id", IntegerType(), True), StructField("product_id", IntegerType(), 
True), StructField("prediction", FloatType(), True)])
dfToSave = spark.createDataFrame(topPredictions, schema)
dfToSave.write \
    .jdbc(jdbcUrl, "morecon.dtb_recommendations",
            properties={"user": CLOUDSQL_USER, "password": CLOUDSQL_PWD})
#[END save_top]


*******************************************************************************************************
# SELECT 
#   p.product_id, p.maker_id , p.spec_color , r.prediction 
# FROM
#   dtb_products p
# INNER JOIN
#   dtb_recommendations r
# ON
#  r.product_id = p.product_id
# WHERE
#   r.customer_id = 312940
# ORDER BY 
#   r.prediction desc
