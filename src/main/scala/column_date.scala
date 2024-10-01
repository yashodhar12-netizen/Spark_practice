import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object column_date {
  def main(Args:Array[String]): Unit={
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val events =List(
      (1, "2024-07-27"),
      (2, "2024-12-2025"),
      (3, "2025-01-01")
    ).toDF("event_id","date")
    events.withColumn("is_holiday",
      when(col("date").isin("2024-12-25", "2025-01-01"), true).otherwise(false)).show()

  }

}
