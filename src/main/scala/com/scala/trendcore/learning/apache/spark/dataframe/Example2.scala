package com.scala.trendcore.learning.apache.spark.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, exp, explode, expr, sum}


object Example2 {

  def main(args: Array[String]): Unit = {

    //https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3741049972324885/2662535171379268/4413065072037724/latest.html

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val session = SparkSession.builder().master("local[*]").enableHiveSupport().getOrCreate();

    import session.sqlContext.implicits._

    //read data from previously written file
    val frame = session.read.parquet("output/example1")
    frame.show()

    //show all columns
    println("Show all columns")
    frame.select("*").show()


    //show department column
    frame.select("department").show()


    //show department,employees column
    frame.select("department", "employees").show()



    println("Exploding struct datatypes")
    frame.select("department.*").show()


    frame.select("department.id","department.name").show()

    explodeEmployees(frame)


    //select distinct
    println("Distinct")
    frame.select($"department.name").distinct().show()



    println("Group By")
    val dataFrame = frame.select(col("department.*"),
      explode(col("employees")).as("employees")
    )
    val frame1 = dataFrame.select("id", "name", "employees.*")

    //This code is not renaming column of sum
    frame1
      .select("name","salary")
      .groupBy(col("name"))
      .sum("salary").alias("sum_of_salaries_department")
      .show()


    println("Using agg -> expr ")

    frame1
      //frame1 -> id,name,firstName,lastName,email,salary
      .select("name","salary")
      //select will return -> name,salary
      .groupBy(col("name"))
      //group by will group with name
      .agg(
        expr("SUM(salary) as dept_salary_sum"),
        expr("COUNT(name) as num_of_entries")
      )
      //group by has to be followed by agg function
      //agg -> agg(expr("<expr>")) will get aggregate columns
      .show()



    println("lastName IS NULL")
    //where clause to find out NULL
    frame1.where("lastName IS NULL").show()


    println("lastName IS NOT NULL")
    //where clause to find out NOT NULL
    frame1.where("lastName IS NOT NULL").show()

    println("Alias ")
    frame1.select(col("id").alias("id_id")).show()

    frame1.select(col("id").as("id_id")).show()


    /*
    withColumn

        When you wanted to add, replace or update multiple columns in Spark DataFrame,
        it is not suggestible to chain withColumn() function as it leads into performance issue
        and recommends to use select() after creating a temporary view on DataFrame
     */
    frame1.withColumn("newIdCol",col("id")).show()


    //window function
  }

  private def explodeEmployees(frame: DataFrame) = {
    val dataFrame = frame.select(col("department.*"),
      explode(col("employees")).as("employees")
    )

    dataFrame.select("id", "name", "employees.*").show()
  }
}
