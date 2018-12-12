import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, functions}
import org.apache.spark.sql.types._

object Shivani_Shukla_task1 {

  def main(args: Array[String]): Unit = {
    val input_file_task1 = args(0)
    val output_file_task1= args(1)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setAppName("StackOverflow Survey")
    conf.setMaster("local[2]")
    val spark_context = new SparkContext(conf)
    val sql_context = new SQLContext(spark_context)

    val df = sql_context.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema", "true")
      .load(input_file_task1)

    val filtered_response = df.filter(df("Salary") =!= "NA" &&  df("Salary") =!= "0").persist()

    val total_responses = df.groupBy("Country").agg(functions.count("Salary")
        .alias("total"))
     .orderBy(functions.asc("Country"))

    val count = df.filter(df("Salary") =!= "NA" && df("Salary") =!= "0").count()

    val removeDF = df.filter(df("Salary") =!= "NA" && df("Salary") =!= "0")
      .groupBy("Country")
      .agg(functions.count("Salary")
        .alias("total"))
      .orderBy(functions.asc("Country"))

    val newNames = Seq("Total", count.toString)
    val dfRenamed = removeDF.toDF(newNames: _*)
    dfRenamed.coalesce(1).write.option("header","true").csv(output_file_task1)


  }
}
