
#########################################################################################################
#
#   PySpark - Load an HBase table (via Phoenix) into a Spark Dataframe
#
#   Usage:
#   spark-submit --jars /usr/hdp/current/phoenix-client/phoenix-client.jar pysparkPhoenixLoad.py
#
#########################################################################################################

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("pysparkPhoenixLoad").setMaster("yarn-client")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.format("org.apache.phoenix.spark").option("table", "OUTPUT_TABLE").option("zkUrl", "sandbox.hortonworks.com:2181/hbase-unsecure").load()
df.show()

#ZEND
