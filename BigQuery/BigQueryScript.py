#!/usr/bin/python
# try:
#     import gcloud
# except ImportError, err:
#     import sys
#     print(sys.path)
#     print err


# try:
# 	from google.cloud import bigquery
# except ImportError, err:
#     import sys
#     print(sys.path)
#     print err


# ['', '/usr/local/lib/python2.7/dist-packages/pip-8.1.2-py2.7.egg', '/usr/lib/python2.7', '/usr/lib/python2.7/plat-x86_64-linux-gnu',
#  '/usr/lib/python2.7/lib-tk', '/usr/lib/python2.7/lib-old', '/usr/lib/python2.7/lib-dynload',
#   '/usr/local/lib/python2.7/dist-packages', '/usr/lib/python2.7/dist-packages'] No module named gcloud    

from gcloud import bigquery
from gcloud.bigquery import job
from gcloud.bigquery.table import *

# Create a new Google BigQuery client using Google Cloud Platform project
# defaults.
bq = bigquery.Client()


# Create a new BigQuery dataset.
reg_dataset = bq.dataset("natality_regression")
reg_dataset.create()

# In the new BigQuery dataset, create a new table.
table = reg_dataset.table(name="regression_input")
# The table needs a schema before it can be created and accept data.
# We create an ordered list of the columns using SchemaField objects.
schema = []
schema.append(SchemaField("weight_pounds", "float"))
schema.append(SchemaField("mother_age", "integer"))
schema.append(SchemaField("father_age", "integer"))
schema.append(SchemaField("gestation_weeks", "integer"))
schema.append(SchemaField("weight_gain_pounds", "integer"))
schema.append(SchemaField("apgar_5min", "integer"))

# We assign the schema to the table and create the table in BigQuery.
table.schema = schema
table.create()

# Next, we issue a query in StandardSQL.
# The query selects the fields of interest.
query = """
SELECT weight_pounds, mother_age, father_age, gestation_weeks,
weight_gain_pounds, apgar_5min
from `bigquery-public-data.samples.natality`
where weight_pounds is not null
and mother_age is not null and father_age is not null
and gestation_weeks is not null
and weight_gain_pounds is not null
and apgar_5min is not null
"""

# We create a query job to save the data to a new table,
qj = job.QueryJob("natality-extract", query, bq)

qj.write_disposition="WRITE_TRUNCATE"
qj.use_legacy_sql = False
qj.destination = table

# 'qj.begin' submits the query.
qj.begin()