/************************************************************************************************************************
*
*   Load HBase table into Spark via Phoenix
*   This script connects to HBase (via Phoenix) and loads the data into a Spark DataFrame. 
*
*   Note: This scripts reads the DF from SparkPhoenixSave.
*   Note: Use Phoenix to create HBase table called OUTPUT_TABLE
*   echo "CREATE TABLE OUTPUT_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);" > /tmp/create_table.sql
*   /usr/hdp/current/phoenix-client/bin/sqlline.py localhost:2181:/hbase-unsecure /tmp/create_table.sql
*
*   Usage:
*   spark-submit --class com.github.zaratsian.SparkPhoenix.SparkPhoenixBulkLoad --jars /tmp/SparkPhoenix-0.0.1.jar /usr/hdp/current/phoenix-client/phoenix-client.jar /tmp/props
*
************************************************************************************************************************/

package com.github.zaratsian.SparkPhoenix;

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.phoenix.spark._

object SparkPhoenixLoad {
    def main(args: Array[String]) {
  
        val sparkConf = new SparkConf().setAppName("SparkPhoenixLoad")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new SQLContext(sc)

        val df = sqlContext.load(
            "org.apache.phoenix.spark",
            Map("table" -> "OUTPUT_TABLE", "zkUrl" -> "seregion01.cloud.hortonworks.com:2181/hbase-unsecure")
            )

        df.show

    }
}

//ZEND
