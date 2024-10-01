import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object nested_column {
  def main(Args:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val inventory = List(
      (1,5),
      (2,15),
      (3,25)
    ).toDF("item_id","quantity")
    inventory.withColumn("stock_level", when(col("quantity")<10, "Low").when(col("quantity").between(10,20),"Medium").otherwise("high")).show()
  }


}
