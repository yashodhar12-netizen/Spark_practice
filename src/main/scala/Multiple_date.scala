import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object Multiple_date {
  def main(Args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val tasks = List(
      (1, "2024-07-01", "2024-07-10"),
      (2, "2024-08-01", "2024-08-15"),
      (3, "2024-09-01", "2024-09-05")
    ).toDF("task_id", "start_date", "end_date")
    tasks.withColumn("task_duration",when(datediff(col("end_date"), col("start_date")) < 7, "Short")
      .when(datediff(col("end_date"), col("start_date")).between(7, 14), "Medium")
      .otherwise("Long")
    ).show()
  }


}
