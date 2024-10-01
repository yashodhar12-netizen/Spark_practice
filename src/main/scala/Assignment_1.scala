import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Assignment_1 {
  def main(Args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function
    val employees = List(
      (1, "John", 28),
      (2, "Jane", 35),
      (3, "Doe", 22)
    ).toDF("id", "name", "age")

    employees.withColumn("is_adult",when(col("age")>=18,"True").otherwise("false")).show()
  }
  //spark.stop()
}