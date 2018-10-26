
import org.apache.spark.sql.{SparkSession, functions => f}
import org.apache.spark.{SparkConf, SparkContext}

object Task3{

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("Word Count 1")
    val sc = new SparkContext(conf)
    val spark = SparkSession.
      builder.
      appName("Simple Application 1").
      master("local[2]").
      getOrCreate()
    val df = spark.read.format("csv").option("header", "true").load(args(0))
    df.createGlobalTempView("TableSurvey")
    val df2 = spark.sql("SELECT Country, REPLACE(Salary,',','') as Salary, SalaryType FROM global_temp.TableSurvey WHERE (Salary != 'NA' AND Salary != '0')")
    val df3 = df2.filter(df2("SalaryType").equalTo("Monthly"))
      .select(df2("Country"),((df2("Salary").cast("double"))*12.0).alias("Salary"))
      .union(df2.filter(df2("SalaryType").equalTo("Weekly"))
        .select(df2("Country"),((df2("Salary").cast("double"))*52.0).alias("Salary")))
      .union(df2.filter(df2("SalaryType").notEqual("Monthly") && df2("SalaryType").notEqual("Weekly"))
        .select(df2("Country"),((df2("Salary").cast("double"))*1.0).alias("Salary")))
    val df4 = df3.groupBy("Country").agg(f.count("Salary"),f.min("Salary").cast("int"), f.max("Salary").cast("int"), f.round(f.avg("Salary").cast("double"),2))
      .orderBy("Country")
    df4.coalesce(1).write.format("com.databricks.spark.csv").option("header","false").mode("append").save(args(1))
  }

}