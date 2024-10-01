import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object conditional_columns {
  def main(Args:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._
    val reviews = List(
      (1, 1),
      (2, 4),
      (3, 5)
    ).toDF("review_id", "rating")
    reviews.withColumn("feedback",
        when(col("rating") < 3, "Bad")
          .when(col("rating").isin(3, 4), "Good")
          .otherwise("Excellent"))
      .withColumn("is_positive",
        when(col("rating") >= 3, true)
          .otherwise(false)).show()

  }

}
