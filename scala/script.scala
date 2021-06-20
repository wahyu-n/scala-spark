import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import spark.implicits._

object FromJsonFile {
  def main(args:Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkLatihan")
      .getOrCreate()

    val sc = spark.sparkContext

    val path = ".data/data.json"
    val df = spark.read.format("json").option("multilines", true).option("path", path).load()
    df.printSchema()
    df.show()
    df.select("color").sort(desc("color"))
    df.show()
  }
}