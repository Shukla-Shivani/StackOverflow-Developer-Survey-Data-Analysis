import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.types._
//import org.apache.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object Task3 {

  def main(args: Array[String]): Unit = {
    val input_file_task3 = args(0)
    val output_file_task3 = args(1)

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
      .load(input_file_task3)


    val filtered_response = df.filter(df("Salary") =!= "NA" &&  df("Salary") =!= "0").persist()
    val salary_casted_filtered_response = filtered_response.withColumn("longSalary", df("Salary").cast(LongType))
      .drop("Salary").withColumnRenamed("longSalary", "Salary")

    val df_with_annual_salary = salary_casted_filtered_response.withColumn("annual_salary", when(col("SalaryType") === "Weekly", col("Salary")*52).otherwise(when(col("SalaryType") === "Monthly", col("Salary")*12).otherwise(col("Salary"))))
    //val calculated_df = df_with_annual_salary.groupBy("Country").agg(count("Salary"), max("annual_salary").cast(LongType), min("annual_salary").cast(LongType), avg("annual_salary").cast(LongType)).orderBy(asc("Country"))
    //calculated_df.show()
    val calculated_df = df_with_annual_salary.groupBy("Country").agg(count("Salary"), min("annual_salary").cast(LongType), max("annual_salary").cast(LongType), avg("annual_salary").cast(LongType)).orderBy(asc("Country"))
    //calculated_df.show()

    calculated_df.coalesce(1).write.option("header","false").csv(output_file_task3)
  }
}
