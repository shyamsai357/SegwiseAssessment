package org.segwise

import org.apache.spark.sql.SparkSession

object Segwise {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Segwise")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/input/records1000")

    df.show()
    df.printSchema()
  }
}
