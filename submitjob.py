#Run The Scripts
# Google Compute Engine instance => master node 
# 1/Using bdutil on the master node
spark-submit \
--driver-class-path mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar \
--jars mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar \
/home/mouna_balghouthi/rectest4.py \
IP \
Dbname \
user \
pass
*****************************************************************************************************************************
Traceback (most recent call last):
  File "/home/rectest4.py", line 62, in <module>
    dfUserRatings  = dfRates.filter(dfRates.customer_id == USER_ID).map(lambda r: r.product_id).collect()
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/sql/dataframe.py", line 841, in __getattr__
AttributeError: 'DataFrame' object has no attribute 'map'


 dfUserRatings  = dfRates.filter(dfRates.customer_id == USER_ID).rdd.map(lambda r: r.product_id).collect()

*****************************************************************************************************************************
  ERROR :

  File "/home/rectest2.py", line 41, in <module>
    dfRates = sqlContext.load(source='jdbc', driver=jdbcDriver, url=jdbcUrl, dbtable=TABLE_RATINGS)
AttributeError: 'SQLContext' object has no attribute 'load'

CHECK :
https://stackoverflow.com/search?q=df+%3D+sqlContext.load%28source%3D%22jdbc%22+

*****************************************************************************************************************************
$ spark-submit \
  --driver-class-path mysql-connector-java-5.1.36-bin.jar \
  --jars mysql-connector-java-5.1.36-bin.jar \
  app_collaborative.py \
  <YOUR_CLOUDSQL_INSTANCE_IP> \
  <YOUR_CLOUDSQL_INSTANCE_NAME> \
  <YOUR_CLOUDSQL_USER> \
  <YOUR_CLOUDSQL_PASSWORD> \
  <YOUR_BEST_RANK> \
  <YOUR_BEST_ITERATION> \
  <YOUR_BEST_REGULATION>

$ spark-submit \
  --driver-class-path mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar \
  --jars mysql-connector-java-5.1.40-bin.jar \
  find_model_collaborative.py \
  <YOUR_CLOUDSQL_INSTANCE_IP> \
  <YOUR_CLOUDSQL_INSTANCE_NAME> \
  <YOUR_CLOUDSQL_USER> \
  <YOUR_CLOUDSQL_PASSWORD>

*****************************************************************************************************************************
# 2/ from a local computer:
$ gcloud beta dataproc jobs submit pyspark \
  --cluster <YOUR_DATAPROC_CLUSTER_NAME> \
  app_collaborative.py \
  <YOUR_CLOUDSQL_INSTANCE_IP> \
  <YOUR_CLOUDSQL_INSTANCE_NAME> \
  <YOUR_CLOUDSQL_USER> \
  <YOUR_CLOUDSQL_PASSWORD> 

gcloud beta dataproc jobs submit pyspark PY_FILE --cluster=CLUSTER 
[--archives=[ARCHIVE,…]] 
[--async] [--bucket=BUCKET] 
[--driver-log-levels=[PACKAGE=LEVEL,…]] 
[--files=[FILE,…]] 
[--labels=[NAME=VALUE,…]] 
[--properties=[PROPERTY=VALUE,…]] 
[--py-files=[PY_FILE,…]] 
[GLOBAL-FLAG …] 
[-- JOB_ARGS …]

$ gcloud beta dataproc jobs submit pyspark /home/mouna_balghouthi/rectest3.py \
  -- cluster=rupdwh \
  -- CLOUDSQL_INSTANCE_IP=104.199.215.232 \
  -- CLOUDSQL_DB_NAME=morecon \
  -- CLOUDSQL_USER=root \
  -- CLOUDSQL_PASSWORD=spark

File Not found Error 
*****************************************************************************************************************************



gcloud dataproc jobs submit pyspark PY_FILE --cluster=CLUSTER [--archives=[ARCHIVE,…]] [--async] [--bucket=BUCKET] [--driver-log-levels=[PACKAGE=LEVEL,…]] [--files=[FILE,…]] [--properties=[PROPERTY=VALUE,…]] [--py-files=[PY_FILE,…]] [GLOBAL-FLAG …] [-- JOB_ARGS …]


$ gcloud dataproc jobs submit spark --cluster dplab \
  --class org.apache.spark.examples.SparkPi \
  --jars file:///usr/lib/spark/examples/jars/spark-examples.jar 1000


spark-submit --cluster=rupdwh --bucket=rupdwh --py-files=hrectest1.py nope

spark-submit --master spark://Moon.local:7077 /Users/Desktop/HybridRS/HybridRecommender.py

gcloud dataproc jobs submit pyspark --cluster rupdwh /Users/Desktop/HybridRS/HybridRecommender.py

# 2/ from a local computer:
$ gcloud compute ssh rupdwh-m

wrong args ? 

$ gcloud beta dataproc jobs submit pyspark \
  --cluster rupdwh \
  --driver-class-path mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar \
  --jars mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar \
  /home/rectest4.py \
 ip \
  instancetestx \
 db \
  user \
  pass
