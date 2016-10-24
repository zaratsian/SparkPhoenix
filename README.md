<h3>Spark to Phoenix (HBase) Example</h3>

<br>This repo contains code to bulkload data from Spark to HBase (via Phoenix).
<br>
<br>The goal is to use Apache Phoenix to speed up bulkloading time, compared to HBase Bulkloading (as I've demonstrated in this repo: https://github.com/zaratsian/SparkHBaseExample
<br>
<br>To BulkLoad HFiles (saved in HDFS) into an HBase table:
<br>hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles path/to/hfile tableName
<br>
<br>Links:
<br><a href="https://phoenix.apache.org/bulk_dataload.html">Phoenix - Bulk CSV Data Loading</a>
<br><a href="https://phoenix.apache.org/phoenix_spark.html">Phoenix - Apache Spark Plugin</a>
