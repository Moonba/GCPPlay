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
# conf = SparkConf().setAppName("MoreConRecALS")
# sc = SparkContext(conf=conf)
# sqlContext = SQLContext(sc)

spark = SparkSession \
        .builder \
        .appName("MoreConRecALS") \
        .getOrCreate()

USER_ID = 170897

CLOUDSQL_INSTANCE_IP = sys.argv[1] #104.199.213.82
CLOUDSQL_DB_NAME = sys.argv[2] #morecon
CLOUDSQL_USER = sys.argv[3] #root
CLOUDSQL_PWD  = sys.argv[4] #m

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
print "dfRates"
dfRates.show()
#[END read_from_sql]
# Get all the ratings rows of our customer
dfUserRatings  = dfRates.filter(dfRates.customer_id ==USER_ID)
print "after filter"
dfUserRatings.show()
dfUserRatings = dfUserRatings.rdd.map(lambda r: r.product_id)
print "after map"
print dfUserRatings
print dfUserRatings.count()
dfUserRatings = dfUserRatings.collect()
print "after collect"
print dfUserRatings[0]
print dfUserRatings[1]
print len(dfUserRatings)
# Returns only the products that have not been rated by our customer
print "rddPotential"
rddPotential  = dfProducts.rdd.filter(lambda x: x[0] not in dfUserRatings)
print rddPotential
print rddPotential.count()
print "pairs Potential"
pairsPotential = rddPotential.map(lambda x: (USER_ID, x[0]))
print pairsPotential
print pairsPotential.count()
#[START split_sets]
rddTraining, rddValidating, rddTesting = dfRates.rdd.randomSplit([6,2,2])
#[END split_sets]
print "rddTraining"
print rddTraining.count()
print(rddTraining)
print(rddTraining.take(3))
#[START predict]
# Build our model with the best found values
# Rating, Rank, Iteration, Regulation
model = ALS.train(rddTraining, 20, 10, 0.1)
# Calculate all predictions
predictions = model.predictAll(pairsPotential).map(lambda p: (str(p[0]), str(p[1]), float(p[2])))
# Take the top 5 ones
topPredictions = predictions.takeOrdered(5, key=lambda x: -x[2])
print(topPredictions)
#[START save_top]
schema = StructType([StructField("customer_id", StringType(), True), StructField("product_id", StringType(),
 True), StructField("prediction", FloatType(), True)])
dfToSave = spark.createDataFrame(topPredictions, schema)
dfToSave.write \
    .jdbc(jdbcUrl, "morecon.dtb_recommendations",
            properties={"user": CLOUDSQL_USER, "password": CLOUDSQL_PWD})