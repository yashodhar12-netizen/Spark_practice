import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object mail {
  def main(Args:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val emails = List(
      (1, "user@gmail.com"),
      (2, "admin@yahoo.com"),
      (3, "info@hotmail.com")
    ).toDF("email_id", "email_address")
    emails.withColumn("email_domain",when(col("email_address").contains("gmail"), "Gmail")
      .when(col("email_address").contains("yahoo"), "Yahoo").otherwise("Hotmail")).show()
  }

}
