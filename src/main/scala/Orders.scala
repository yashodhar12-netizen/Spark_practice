import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object Orders {
  def main(Args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val orders = List(
      (1, 5, 100),
      (2, 10, 150),
      (3, 20, 300)
    ).toDF("order_id", "quantity", "total_price")
    orders.withColumn("orders_type",when(col("quantity") < 10 && col("total_price") < 200, "Small & Cheap")
      .when(col("quantity") >= 10 && col("total_price") < 200, "Bulk & Discounted")
      .otherwise("Premium Order")).show()

  }

}
