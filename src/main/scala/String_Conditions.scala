import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object String_Conditions {
  def main(Args:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")
    ).toDF("doc_id", "content")
    documents.withColumn("content_category",when(col("content").contains("fox"),"Animal Related").when(col("content").contains("Lorem"), "Placeholder Text").when(col("content").contains("Spark"), "Tech Related").otherwise("Other")).show()
  }


}
