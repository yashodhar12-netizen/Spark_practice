import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object column3 {
  def main(Args:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val transactions = List(
      (1, 1000),
      (2, 200),
      (3, 5000)
    ).toDF("transaction_id","amount")

    transactions.withColumn("category",when(col("amount")>1000,"High").when(col("amount").between(500,1000),"Medium").otherwise("Low")).show()}
}
