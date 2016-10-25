
/************************************************************************************************************************
*
*   Save Spark DataFrame to HBase (via Phoenix)
*   This script creates a small Spark Dataframe (in scala) and then save the data to HBase (via Phoenix). 
*
*   Note: Use Phoenix SQL to create HBase table called OUTPUT_TABLE
*   echo "CREATE TABLE OUTPUT_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);" > /tmp/create_table.sql
*   /usr/hdp/current/phoenix-client/bin/sqlline.py localhost:2181:/hbase-unsecure /tmp/create_table.sql
*
*   Usage:
*   spark-submit --class com.github.zaratsian.SparkPhoenix.SparkPhoenixSave --jars /tmp/SparkPhoenix-0.0.1.jar /usr/hdp/current/phoenix-client/phoenix-client.jar /tmp/props
*
************************************************************************************************************************/

package com.github.zaratsian.SparkPhoenix;

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.phoenix.spark._

object SparkPhoenixSave {
    def main(args: Array[String]) {
  
        val sparkConf = new SparkConf().setAppName("SparkPhoenixSave")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

        val rdd = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))
        val df = rdd.toDF("id","col1","col2")
        df.show()

        // Save to OUTPUT_TABLE
        df.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "OUTPUT_TABLE", "zkUrl" -> "seregion01.cloud.hortonworks.com:2181/hbase-unsecure" ))
    }
}

//ZEND
