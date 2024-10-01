import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object Multiple_nested {
  def main(Args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val sales = List(
      (1, 100),
      (2, 1500),
      (3, 300)
    ).toDF("sale_id", "amount")
    sales.withColumn("discount",when(col("amount")<200, 0).when(col("amount").between(200,1000), 10).otherwise(20)).show()
  }


}
