package crimes

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TestHelper extends FunSuite {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
  }
}
