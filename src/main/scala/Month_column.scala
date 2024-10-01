import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object Month_column {
  def main(Args:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val payments = List(
      (1, "2024-07-15"),
      (2, "2024-12-25"),
      (3, "2024-11-01")
    ).toDF("payment_id", "payment_date")
    payments.withColumn("payment_month", month(to_date(col("payment_date"), "yyyy-MM-dd")))
      .withColumn("quarter",
        when(col("payment_month").between(1, 3), "Q1")
          .when(col("payment_month").between(4, 6), "Q2")
          .when(col("payment_month").between(7, 9), "Q3")
          .otherwise("Q4")).show()
  }

}
