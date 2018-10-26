import java.io.FileWriter

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Task2{

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)
    val spark = SparkSession.
      builder.
      appName("Simple Application").
      master("local[2]").
      getOrCreate()
    val df = spark.read.format("csv").option("header", "true").load(args(0))
    val df2 = df.filter(df("Salary").notEqual("NA") && df("Salary").notEqual("0")).select("Country").rdd.map(row => (row(0), 1))
    var std_part = df2.mapPartitions(iter => Array(iter.size).iterator, true)
    val std_value = std_part.collect().toList.mkString(",")
    print(std_value)
    val t1 = System.currentTimeMillis()
    df2.reduceByKey((a, b) => a + b).collect()
    val duration = (System.currentTimeMillis() - t1)
    print (duration)
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val df1 = spark.read.format("csv").option("header", "true").load(args(0))
    val df21 = df1.filter(df1("Salary").notEqual("NA") && df1("Salary").notEqual("0")).select("Country").repartition(df1("Country")).repartition(2).rdd.map(row => (row(0), 1))
    var std2_part = df21.mapPartitions(iter => Array(iter.size).iterator, true)
    val std2_value = std2_part.collect().toList.mkString(",")
    val t11 = System.currentTimeMillis()
    df21.reduceByKey((a, b) => a + b).collect()
    val duration1 = (System.currentTimeMillis() - t11)
    val file1 = new FileWriter(args(1), true)

    try {
      file1.write("standard," + std_value + "," + (duration).toString() + "\n")
      file1.write("partition," + std2_value + "," + (duration1).toString())
    }
    finally file1.close()
  }

}