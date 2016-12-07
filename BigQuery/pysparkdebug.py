
from datetime import datetime
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

def vector_from_inputs(r):
  return (r["weight_pounds"], Vectors.dense(float(r["mother_age"]),
                                            float(r["father_age"]),
                                            float(r["gestation_weeks"]),
                                            float(r["weight_gain_pounds"]),
                                            float(r["apgar_5min"])))

spark = SparkSession \
        .builder \
        .appName("DebugTesto") \
        .getOrCreate()

bucket = spark._jsc.hadoopConfiguration().get("fs.gs.system.bucket")
project = spark._jsc.hadoopConfiguration().get("fs.gs.project.id")

todays_date = datetime.strftime(datetime.today(), "%Y-%m-%d-%H-%M-%S")
input_directory = "gs://{}/tmp/natality-{}".format(bucket, todays_date)

conf = {
   
    "mapred.bq.project.id": project,
    "mapred.bq.gcs.bucket": bucket,
    "mapred.bq.temp.gcs.path": input_directory,
    "mapred.bq.input.project.id": project,
    "mapred.bq.input.dataset.id": "natality_regression",
    "mapred.bq.input.table.id": "regression_input",
}


table_data = spark.sparkContext.newAPIHadoopRDD(
    "com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "com.google.gson.JsonObject",
    conf=conf)

print "\n\n\n\n\n\n Schema \n\n\n\n\n\n"
table_data.first()
#nothing shown
table_data.count()
table_data.isEmpty()
table_data.ExportFileFormat()

print "\n\n\n\n\n\n check done ? \n\n\n\n\n\n"

table_json = table_data.map(lambda x: x[1])

print "\n\n\n\n\n\n table_json.collect ? \n\n\n\n\n\n"
sorted(table_json.collect())

print "\n\n\n\n\n\n natality_data ? \n\n\n\n\n\n"


table_json = table_data.map(lambda x: x[1])
