 import org.apache.spark.SparkConf
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession

  object column2{
    def main(Args:Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("Yashodhar")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._ // for toDF function
      val grades = List(
        (1, 85),
        (2, 42),
        (3, 73)
      ).toDF("student_id", "score")

      grades.withColumn("Result",when(col("score")>=50,"Pass").otherwise("Fail")).show()
    }
    //spark.stop()
}
