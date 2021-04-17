package exercise

import org.apache.spark.sql.SparkSession

object SessionBuilder {

  def session(): SparkSession = {
    SparkSession
      .builder()
      .appName("Joins")
      .config("spark.master", "local")
      .getOrCreate()
  }

}
