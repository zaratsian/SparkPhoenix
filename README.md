<h3>Spark to Phoenix (HBase) Example</h3>

<br>This repo contains Spark code that will bulkload data from Spark into HBase (via Phoenix). I've also included Spark code (SparkPhoenixSave.scala) to Save a DataFrame directly to HBase, via Phoenix. Similarly, there is code (SparkPhoenixLoad.scala) that'll load data from HBase, via Phoenix, into a Spark dataframe.
<br>
<br>The goal is to use Apache Phoenix to speed up bulkloading time, compared to HBase Bulkloading (as I've demonstrated in this repo: https://github.com/zaratsian/SparkHBaseExample
<br>
<br>To BulkLoad HFiles (saved in HDFS) into an HBase table:
<br>```hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles path/to/hfile tableName```
<br>
<br>Links:
<br><a href="https://phoenix.apache.org/bulk_dataload.html">Phoenix - Bulk CSV Data Loading</a>
<br><a href="https://phoenix.apache.org/phoenix_spark.html">Phoenix - Apache Spark Plugin</a>
