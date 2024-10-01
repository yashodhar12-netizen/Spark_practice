import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object column4 {
def main(Args:Array[String]): Unit ={
  val spark = SparkSession.builder()
    .appName("Yashodhar")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val products = List(
    (1, 30.5),
    (2, 150.75),
    (3, 75.25)
  ).toDF("product_id","price")

  products.withColumn("price_range",when(col("price")<50, "Cheap").when(col("price").between(50,100),"Moderate").otherwise("Expensive")).show()

}
}
