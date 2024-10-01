import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object Multiple_Conditional {
  def main(rgs:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val scores = List(
      (1, 85, 92),
      (2, 58, 76),
      (3, 72, 64)
    ).toDF("student_id", "math_score", "english_score")
    scores.withColumn("math_grade",when(col("math_score") > 80, "A").when(col("math_score") >=60 && col("math_score") <=80, "B").otherwise("C")) .withColumn("english_grade",
      when(col("english_score") > 80, "A")
        .when(col("english_score") >= 60 && col("english_score") <= 80, "B")
        .otherwise("C")).show()
  }

}
