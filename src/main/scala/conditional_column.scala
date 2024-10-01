import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object conditional_column {
  def main(rgs:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("Yashodhar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val weather = Seq(
      (1, 25, 60),
      (2, 35, 40),
      (3, 15, 80)
    ).toDF("day_id", "temperature", "humidity")
    weather.withColumn("is_hot", col("temperature") > 30).show()
    weather.withColumn("is_humid",col("humidity") > 50).show()

  }

}
