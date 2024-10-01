import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Date_manipulation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Add Season Column Example")
      .master("local[*]") // Change as needed for your Spark setup
      .getOrCreate()

    import spark.implicits._

    // Create the orders DataFrame
    val orders = Seq(
      (1, "2024-07-01"),
      (2, "2024-12-01"),
      (3, "2024-05-01")
    ).toDF("order_id", "order_date")

    // Convert order_date to DateType
    val ordersWithDate = orders.withColumn("order_date", to_date($"order_date"))

    // Add the season column
    val ordersWithSeason = ordersWithDate.withColumn("season",
      when(month($"order_date").isin(6, 7, 8), "Summer")
        .when(month($"order_date").isin(12, 1, 2), "Winter")
        .otherwise("Other")
    )

    // Show the result
    ordersWithSeason.show()

    spark.stop()
  }
}
