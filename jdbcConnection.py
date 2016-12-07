# Original Script 

conf = SparkConf().setAppName("MyApp")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Read the data from the Cloud SQL
# Create dataframes
#[START read_from_sql]
jdbcDriver = 'com.mysql.jdbc.Driver'
jdbcUrl    = 'jdbc:mysql://%s:3306/%s?user=%s&password=%s' % (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME, CLOUDSQL_USER, CLOUDSQL_PWD)

dfProducts = sqlContext.load(source='jdbc', driver=jdbcDriver, url=jdbcUrl, dbtable=TABLE_ITEMS)
dfRates = sqlContext.load(source='jdbc', driver=jdbcDriver, url=jdbcUrl, dbtable=TABLE_RATINGS)
#[END read_from_sql]

************************************************************************************************************************
1/sqlContext

a)
dfRates = sqlContext.read \
    .format("jdbc") \
    .options(url="jdbc:mysql://130.211.246.224:3306/morecon?user=x&password=xx",
             dbtable="dtb_ratings",
             driver="com.mysql.jdbc.Driver") \
    .load()
dfRates.count()

a)
jdbcDriver = 'com.mysql.jdbc.Driver'
jdbcUrl    = 'jdbc:mysql://%s:3306/%s?user=%s&password=%s' % (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME, CLOUDSQL_USER, CLOUDSQL_PWD)
dfRates = sqlContext.read.load(source='jdbc', driver=jdbcDriver, url=jdbcUrl, dbtable='dtb_ratings')

a)


jdbcUrl = 'jdbc:mysql://%s:3306/%s' % (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME)
dfRates = sqlContext.read.jdbc(url=url, table="dtb_ratings", properties={"user": "", "password": ""})
sqlContext.read.jdbc(url=url, table="baz", properties=properties)

a)

dfRates = (sqlContext.read.format("jdbc")
    .options(url=jdbcUrl, dbtable="TABLE_RATINGS")
    .load())

dfRates = (sqlContext.read.format("jdbc")
    .options(url=url, dbtable="baz", **properties)
    .load())

dfRates.show();

==> Fail 

************************************************************************************************************************

2/ SparkSession
spark = SparkSession \
        .builder \
        .appName("MyApp") \
        .getOrCreate()

def jdbc_dataset_example(spark):
    # $example on:jdbc_dataset$
    # Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    # Loading data from a JDBC source
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:dbserver") \
        .option("dbtable", "schema.tablename") \
        .option("user", "username") \
        .option("password", "password") \
        .load()

    jdbcDF2 = spark.read \
        .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
              properties={"user": "username", "password": "password"})

    # Saving data to a JDBC source
    jdbcDF.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:dbserver") \
        .option("dbtable", "schema.tablename") \
        .option("user", "username") \
        .option("password", "password") \
        .save()

    jdbcDF2.write \
        .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
              properties={"user": "username", "password": "password"})
    # $example off:jdbc_dataset$
        
