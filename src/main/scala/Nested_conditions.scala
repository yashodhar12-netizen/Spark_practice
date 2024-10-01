import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object Nested_conditions {
  def main(Args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val employees = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    ).toDF("employee_id", "age", "salary")
    employees.withColumn("category", when(col("age") < 30 && col("salary") < 35000, "Young & Low Salary")
      .when(col("age").between(30, 40) && col("salary").between(35000, 45000), "Middle Aged & Medium Salary")
      .otherwise("Old & High Salary")).show()
  }
}
