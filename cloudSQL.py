1/ create an instancetestx

https://console.cloud.google.com/sql/instances?project=r-n-dmanagement


https://github.com/GoogleCloudPlatform/spark-recommendation-engine
#create-and-configure-google-cloud-sql-first-generation-access

access control
Userschange root password
https://console.cloud.google.com/home/dashboard?project=r-n-dmanagement


2 go google console page
open shell 
connect to the instance 

#Connect to your instance using the MySQL client in the Cloud Shell on the web UI
gcloud beta sql connect instancetest6 --user=
psd?

>Mysql

CREATE DATABASE morecon ;

Import morecon.sql from instance page

>Mysql
show database ;


>Mysql
USE morecon ; 
show tables ; 

create table browsing_history 
(   time INT NOT NULL,
  td_referrer varchar(255),
   td_path varchar(255),
  td_url varchar(255)
  )

create table users_browsing_history
( time INT NOT NULL,
  td_ip TEXT,
  td_user_agent TEXT,
  td_referrer varchar(255),
  td_path varchar(255),
  td_url varchar(255)
  )

CREATE TABLE dtb_review
(   customer_id INT NOT NULL,
	product_id INT NOT NULL,
	rating FlOAT NOT NULL	);

CREATE TABLE dtb_ratings
( 	customer_id INT NULL,
	product_id INT NOT NULL,
	recommend_level INT NOT NULL	
	);


CREATE TABLE dtb_ratings
(   review_id INT NOT NULL AUTO_INCREMENT,
	customer_id INT NOT NULL,
	product_id INT NOT NULL,
	rating INT NOT NULL,
	PRIMARY KEY(review_id));


CREATE TABLE dtb_recommendations 
( 
  customer_id INT NOT NULL,
  product_id INT NOT NULL,
  prediction float,
  
  PRIMARY KEY(customer_id, product_id),
  FOREIGN KEY (product_id)
  	REFERENCES dtb_products(product_id) 
);

CREATE TABLE recommendations 
( 
  customer_id varchar(255),
  product_id varchar(255),
  prediction float,
  
  PRIMARY KEY(customer_id, product_id)
);

CREATE TABLE recommendations
(
  userId varchar(255),
  accoId varchar(255),
  prediction float,
  PRIMARY KEY(userId, accoId),
  FOREIGN KEY (accoId)
    REFERENCES Accommodation(id)
);

Go to instance page => Import morecon data + dtb_ratings
run script CloudTutorial2.py

*****************************************************************************************************************************
CONNECT FROM MASTER NODE 

mysql --host=CLOUDSQL_INSTANCE_IP --user=moo --password
*****************************************************************************************************************************
#Download JDBC CONNECTOR 

# to download the file to the current directory.

$ wget http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.40.zip -O temp.zip
$ unzip temp.zip
rm temp.zip


wget https://storage.googleapis.com/hadoop-tools/bdutil/bdutil-latest.zip -0 temp1.zip
unzip temp1.zip
rm temp1.zip


#PATH
export CLASSPATH=/home/mouna_balghouthi/mysql-connector-java-5.1.40-bin.jar:$CLASSPATH

#JDBC Connector ADD to /etc/spark/conf/spark-defaults.conf

spark.driver.extraClassPath /usr/share/java/mysql-connector-java-5.1.40-bin.jar
spark.executor.extraClassPath /usr/share/java/mysql-connector-java-5.1.40-bin.jar


# Get the data from Cloud SQL
# The Spark SQL context lets you easily connect to a Cloud SQL instance through the JDBC connector. 
# The loaded data is in DataFrame format.
jdbcDriver = 'com.mysql.jdbc.Driver'
jdbcUrl    = 'jdbc:mysql://%s:3306/%s?user=%s&password=%s' % (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME, CLOUDSQL_USER, CLOUDSQL_PWD)
jdbcUrl    = 'jdbc:mysql://%s:3306/%s?user=%s&password=%s' % (104.199.213.82, morecon, root, CLOUDSQL_PWD )
dfAccos = sqlContext.load(source='jdbc', driver=jdbcDriver, url=jdbcUrl, dbtable=TABLE_ITEMS)
dfRates = sqlContext.load(source='jdbc', driver=jdbcDriver, url=jdbcUrl, dbtable=TABLE_RATINGS)

# Install bdutil
export PATH="/home/mouna_balghouthi/bdutil-1.3.5:$PATH"
/home/mouna_balghouthi/bdutil-1.3.5/bdutil

*****************************************************************************************************************************

# Setup A CLUSTER using bdutil
Cluster

Follow these steps to set up Apache Spark:

1/Download bdutil from https://cloud.google.com/hadoop/downloads.

2/Change your environment values as described in the documentation:
a. CONFIGBUCKET="your_root_bucket_name"
b. PROJECT="your-project"

3/Deploy your cluster and log into the Hadoop master instance.

3.1/
$ bdutil deploy -e /home/mouna_balghouthi/bdutil-1.3.5/extensions/spark/spark_env.sh
*************************************
ERROR 1:
(gcloud.compute.instances.create) Some requests did not succeed:
 - Insufficient Permission

solution 
gcloud auth login 

ERROR 2:
(gcloud.compute.instances.create) Some requests did not succeed:
 - Invalid value 'zone(unset)'. Values must match the following regular expression: '[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?'
 sol:

vi .bashrc
export CLOUDSDK_COMPUTE_ZONE=asia-northeast1-b
export CLOUDSDK_COMPUTE_REGION=asia-northeast1

gcloud compute project-info add-metadata \
    --metadata google-compute-default-region=asia-northeast1,google-compute-default-zone=asia-northeast1-b	
//above didnt work 

ERROR 3 :
(gcloud.compute.instances.create) Some requests did not succeed:
 - Invalid value for field 'resource.disks[0].initializeParams.sourceImage': 
 'https://www.googleapis.com/compute/v1/projects/r-n-dmanagement/global/images/debian-7-backports'.
 The referenced image resource cannot be found.

*************************************
3.2/
$ ./bdutil shell
Notes :

Using the bdutil shell is equivalent to using the SSH command-line interface to connect to the instance.
bdutil uses Google Cloud Storage as a file system, which means that all references to files are relative to the CONFIGBUCKET folder.
[src : https://github.com/GoogleCloudPlatform/spark-recommendation-engine]





