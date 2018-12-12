import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter

object Shivani_Shukla_task2 {

  def main(args: Array[String]): Unit = {

    val input_file_task2 = args(0)
    val output_file_task2 = args(1)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setAppName("StackOverflow Survey")
    conf.setMaster("local[2]")
    val spark_context = new SparkContext(conf)
    val sql_context = new SQLContext(spark_context)

    import sql_context.implicits._

    val df = sql_context.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema", "true")
      .load(input_file_task2)

    val filtered_response = df.filter(df("Salary") =!= "NA" &&  df("Salary") =!= "0").persist()

    val total_responses = df.groupBy("Country")
      .agg(functions.count("Salary")
      .alias("total"))
      .orderBy(functions.asc("Country"))

    val task1_DF = df.filter(df("Salary") =!= "NA" && df("Salary") =!= "0")

    //For standard
    //standard start time
    val standard_start_time = System.currentTimeMillis()

    val standard_repartitioned_rdd = task1_DF.rdd.repartition(2)

    //standard end time
    val standard_end_time = System.currentTimeMillis()

    val standard_time_span = standard_end_time - standard_start_time

    val standard_records = standard_repartitioned_rdd
      .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")

    //Row0
    val standard_map1 = standard_records.select(col("number_of_records")).first.getInt(0)

    //Row1
    val standard_map2 = standard_records.select("number_of_records").collect


    //For Partition
    //partition start time
    val partition_start_time = System.currentTimeMillis()

    val partition_repartitioned = task1_DF.repartition(2, $"Country")
    val partition_repartitioned_rdd = partition_repartitioned.rdd

    //partition end time
    val partition_end_time = System.currentTimeMillis()

    val partition_time_span = partition_end_time - partition_start_time

    val partition_records = partition_repartitioned_rdd
      .mapPartitionsWithIndex{case (i,rows) => Iterator((i, rows.size))}
      .toDF("partition_number","number_of_records")

    //Partition - Row0
    val partition_map1 = partition_records.select("number_of_records").first.getInt(0)

    //Partition - Row1
    val partition_map2 = partition_records.select("number_of_records").collect

    import java.io.BufferedWriter
    import java.io.FileWriter
    val out = new BufferedWriter(new FileWriter(output_file_task2))

    val rec1 = "standard"+","+standard_map1+","+standard_map2(1).get(0)+","+standard_time_span

    val rec2 = "partition"+","+partition_map1+","+partition_map2(1).get(0)+","+partition_time_span

    out.write(rec1)
    out.newLine()
    out.write(rec2)
    out.close()

//    println("standard", standard_map1, standard_map2(1).get(0), standard_time_span)
//    println("partition", partition_map1, partition_map2(1).get(0),partition_time_span)



  }
}