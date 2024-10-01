import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object Boolean_values {
  def main(Args:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val logins = List(
      (1, "09:00"),
      (2, "18:30"),
      (3, "14:00")
    ).toDF("login_id", "login_time")
    logins.withColumn("is_morning", when(to_timestamp(col("login_time"), "HH:mm") < to_timestamp(lit("12:00"), "HH:mm"), true)
      .otherwise(false)).show()
  }

}
