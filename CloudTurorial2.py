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
        .appName("MyApp") \
        .getOrCreate()


USER_ID = 272143 #602182 #sys.argv[] 

CLOUDSQL_INSTANCE_IP = sys.argv[1] 
CLOUDSQL_DB_NAME = sys.argv[2] 
CLOUDSQL_USER = sys.argv[3] 
CLOUDSQL_PWD  = sys.argv[4] 

BEST_RANK = 20 #int(sys.argv[5]) 
BEST_ITERATION = 10 #int(sys.argv[6])
BEST_REGULATION = 0.1 #float(sys.argv[7]) 

TABLE_ITEMS  = "dtb_products"
TABLE_RATINGS = "dtb_review"
TABLE_RECOMMENDATIONS = "dtb_recommendations"

# Read the data from the Cloud SQL
# Create dataframes
#[START read_from_sql]

jdbcUrl = 'jdbc:mysql://%s:3306/%s' % (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME)

dfRates = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", 'morecon.dtb_review') \
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

dfUserRatings  = dfRates.filter(dfRates.customer_id == USER_ID)

dfUserRatings = dfUserRatings.rdd.map(lambda r: r.product_id)

dfUserRatings = dfUserRatings.collect()

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
model = ALS.train(rddTraining, BEST_RANK, BEST_ITERATION, BEST_REGULATION)

# Calculate all predictions
predictions = model.predictAll(pairsPotential).map(lambda p: (str(p[0]), str(p[1]), float(p[2])))

# Take the top 5 ones
topPredictions = predictions.takeOrdered(5, key=lambda x: -x[2])
print(topPredictions)


#[START save_top]
schema = StructType([StructField("customer_id", StringType(), True), StructField("product_id", StringType(), True), StructField("prediction", FloatType(), True)])

dfToSave = spark.createDataFrame(topPredictions, schema)

dfToSave.write \
    .jdbc(jdbcUrl, "morecon.dtb_recommendations",
            properties={"user": CLOUDSQL_USER, "password": CLOUDSQL_PWD})

#[END save_top]
