import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object Task1{

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
  //"/Users/krishmehta/Desktop/DataMining/stack-overflow-2018-developer-survey/survey_results_public.csv"
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
    val t1 = System.nanoTime
    import spark.implicits._
    val df = spark.read.format("csv").option("header", "true").load(args(0)).repartition(20)
    val df2 = df.filter(df("Salary").notEqual("NA") && df("Salary").notEqual("0")).select("Country")
    val temp1 = Seq(("Total", df2.count()))
    val temp = df2.groupBy("Country").count().orderBy("Country")
    val appended = temp1.toDF().union(temp)
    appended.coalesce(1).write.format("com.databricks.spark.csv").option("header","false").mode("append").save(args(1))
  }

}