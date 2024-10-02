import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object assignment2 {
  def main(Args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val students = Seq(
      (1, "Alice", 92, "Math"),
      (2, "Bob", 85, "Math"),
      (3, "Carol", 77, "Science"),
      (4, "Dave", 65, "Science"),
      (5, "Eve", 50, "Math"),
      (6, "Frank", 82, "Science")

    ).toDF("student_id", "name", "score", "subject")
    val df1 = students.withColumn("grade", when($"score" >= 90, "A")
      .when($"score" >= 80 && $"score" < 90, "B")
      .when($"score" >= 70 && $"score" < 80, "C")
      .when($"score" >= 60 && $"score" < 70, "D")
      .otherwise("F")).show()
    students.groupBy("subject").agg(
      avg("score").alias("avg_score"),max("score").alias("maximun_score"),min("score").alias("minimum_score")
    ).show()


  }
}