
/*******************************************************************************************************

This code does the following:
  1) Creates an arbitrary RDD with 1 million records in KeyValue format.
  2) Initializes an HBase configuration and job instance.
  3) Creates empty HBase Table if it does not exist.
  4) Saves the RDD as Phoenix formatted HFiles into HDFS.

Usage:
spark-submit --class com.github.zaratsian.SparkPhoenix.SparkPhoenixBulkLoad --jars /tmp/SparkPhoenix-0.0.1.jar /usr/hdp/current/phoenix-client/phoenix-client.jar /tmp/props

********************************************************************************************************/  

package com.github.zaratsian.SparkPhoenix;

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg

import scala.collection.mutable.HashMap
import scala.io.Source.fromFile
import scala.collection.JavaConverters._

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableMapReduceUtil}

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.text.SimpleDateFormat
import java.util.Arrays
import java.util.Date
import java.util.Calendar
import java.lang.String
import util.Random

object SparkPhoenixBulkLoad{
 
    def main(args: Array[String]) {

        val start_time = Calendar.getInstance()
        println("[ *** ] Start Time: " + start_time.getTime().toString)

        /***************************************************************
        *   Parameters
        ****************************************************************/
        val props = getProps(args(0))
        val records_to_simulate = props.getOrElse("records_to_simulate", "1000000").toInt
        val htablename          = props.getOrElse("htablename", "phoenixtable")
        val columnfamily        = props.getOrElse("columnfamily", "cf")
        val hfile_output        = props.getOrElse("hfile_output", "/tmp/phoenix_files")

        val sparkConf = new SparkConf().setAppName("SparkPhoenixHFiles")
        val sc = new SparkContext(sparkConf)

        /***************************************************************
        *   Create HBase Table if it does not exist
        ****************************************************************/
        println("[ *** ] Creating HBase Configuration")
        val hConf = HBaseConfiguration.create()
        hConf.set("zookeeper.znode.parent", "/hbase-unsecure")
        hConf.set(TableInputFormat.INPUT_TABLE, htablename)

        val table = new HTable(hConf, htablename)

        val admin = new HBaseAdmin(hConf)

        if(!admin.isTableAvailable(htablename)) {
            println("[ ***] Creating HBase Table (" + htablename + ")")
            val hTableDesc = new HTableDescriptor(htablename)
            hTableDesc.addFamily(new HColumnDescriptor(columnfamily.getBytes()))
            admin.createTable(hTableDesc)
        }else{
            println("[ *** ] HBase Table ( " + htablename + " ) already exists!")
        }


        /***************************************************************
        *   Simulate Data
        ****************************************************************/
        println("[ *** ] Simulating " + records_to_simulate.toString() + " records...")
        val rdd = sc.parallelize(1 to records_to_simulate)

        println("[ *** ] Creating KeyValues")
        val rdd_out = rdd.map(x => {
            val kv: KeyValue = new KeyValue( Bytes.toBytes(x), "cf".getBytes(), "c1".getBytes(), x.toString.getBytes() )
            (new ImmutableBytesWritable( Bytes.toBytes(x) ), kv)
        })

        println("[ *** ] Printing simulated data (first 10 records): ")
        rdd_out.map(x => x._2.toString).take(10).foreach(x => println(x))

        /***************************************************************
        *   Write to HFiles
        ****************************************************************/
        println("[ *** ] Setting up HBase configuration")
        val conf = HBaseConfiguration.create()
        val job: Job = Job.getInstance(conf, "phoenixbulkload")

        job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setMapOutputValueClass(classOf[KeyValue])

        TableMapReduceUtil.initCredentials(job)

        val htable: HTable = new HTable(conf, htablename)

        println("[ *** ] Setting up HFile Incremental Load")
        HFileOutputFormat2.configureIncrementalLoad(job, htable)

        println("[ *** ] Saving HFiles to HDFS ("+hfile_output.toString()+")")
        rdd_out.saveAsNewAPIHadoopFile(
            hfile_output,
            classOf[ImmutableBytesWritable],
            classOf[Put],
            classOf[HFileOutputFormat2],
            conf)

        // Print Runtime
        val end_time = Calendar.getInstance()
        println("[ *** ] End Time: " + end_time.getTime().toString)
        println("[ *** ] Total Runtime: " + ((end_time.getTimeInMillis() - start_time.getTimeInMillis()).toFloat/1000).toString + " seconds")   
        
        
        sc.stop()
    
    
    }  
    
    
    def convertScanToString(scan : Scan) = {
        val proto = ProtobufUtil.toScan(scan);
        Base64.encodeBytes(proto.toByteArray());
    }
    
    
    def getArrayProp(props: => HashMap[String,String], prop: => String): Array[String] = {
        return props.getOrElse(prop, "").split(",").filter(x => !x.equals(""))
    }
    
    
    def getProps(file: => String): HashMap[String,String] = {
        var props = new HashMap[String,String]
        val lines = fromFile(file).getLines
        lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
        props
    }

}

//ZEND
