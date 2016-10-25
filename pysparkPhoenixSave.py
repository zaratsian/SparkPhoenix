
#########################################################################################################
#
#   PySpark - Save a Spark Dataframe as an HBase table (via Phoenix)
#
#   Usage:
#   spark-submit --jars /usr/hdp/current/phoenix-client/phoenix-client.jar pysparkPhoenixSave.py
#
#########################################################################################################

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("pysparkPhoenixSave").setMaster("yarn-client")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

rdd = sc.parallelize([(7L, "7", 7), (8L, "8", 8), (9L, "9", 9)])
df  = rdd.toDF(["id","col1","col2"])

df.show()

df.write \
    .format("org.apache.phoenix.spark") \
    .mode("overwrite") \
    .option("table", "OUTPUT_TABLE") \
    .option("zkUrl", "seregion01.cloud.hortonworks.com:2181") \
    .save()

#ZEND
