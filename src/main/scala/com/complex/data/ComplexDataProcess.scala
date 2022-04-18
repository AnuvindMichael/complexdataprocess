package com.complex.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source
object ComplexDataProcess {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().master("local[*]").appName("ComplexData").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("=========Lets Starts ==========")

    val data = spark.read.format("json").option("multiline","true")
    .load("file:///D://Practice/complex.json")
    data.printSchema()

    val data2 = data.withColumn("results",expr("explode(results)"))
    data2.printSchema()

    println("======Final flatten df========")

    val flattendf = data2.select(
        col("nationality"),
        col("results.user.SSN"),
        col("results.user.cell"),
        col("results.user.dob"),
        col("results.user.email"),
        col("results.user.gender"),
        col("results.user.location.*"),
        col("results.user.md5"),
        col("results.user.name.*"),
        col("results.user.password"),
        col("results.user.phone"),
        col("results.user.picture.*"),
        col("results.user.registered"),
        col("results.user.salt"),
        col("results.user.sha1"),
        col("results.user.sha256"),
        col("results.user.username"),
        col("seed"),
        col("version")
    )
    flattendf.printSchema()

    data.show(10)

    println("=====Array to struct=======")

    data2.show(10)

    println("==========Fully flatten data==============")

    flattendf.show(10)

    println("=========remove numerical from username=======")

    val nonuser = flattendf
      .withColumn("username",regexp_replace(col("username"),"[0-9]",""))
    nonuser.show(10)

    println("=========reverting data to complex========")

    val maindata = flattendf.groupBy("nationality","seed","version")
    .agg(collect_list(
      struct(struct(
        col("SSN"),
        col("cell"),
        col("dob"),
        col("email"),
        col("gender"),
        struct(
          col("city"),
          col("state"),
          col("street"),
          col("zip")
        ).alias("location"),
        col("md5"),
        struct(
          col("first"),
          col("last"),
          col("title")
        ).alias("name"),
        col("password"),
        col("phone"),
        struct(
          col("large"),
          col("medium"),
          col("thumbnail")

        ).alias("picture"),
        col("registered"),
        col("salt"),
        col("sha1"),
        col("username"),
        col("sha256")

      ).alias("user"))

    ).as("results"))

    maindata.printSchema()

    maindata.show(10)

    println("=========READ COMPLEX XML DATA==========")

    val xmldata = spark.read.format("com.databricks.spark.xml").option("rowTag","POSLog")
      .load("file:///D:Practice/complexxml.xml")

    xmldata.printSchema()
    xmldata.show(10)












  }

}
