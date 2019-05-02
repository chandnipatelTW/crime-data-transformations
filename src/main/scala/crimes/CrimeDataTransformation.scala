package crimes

import org.apache.spark.sql.{DataFrame, SparkSession}

object CrimeDataTransformation {
  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
  }

  private val laDF: DataFrame = {
    val fileName = "./src/test/resources/LA.parquet"
    spark.read.parquet(fileName)
  }

  private val dallasDF: DataFrame = {
    val fileName = "./src/test/resources/Dallas.parquet"
    spark.read.parquet(fileName)
  }

  private val philadelphiaDF: DataFrame = {
    val fileName = "./src/test/resources/Philadelphia.parquet"
    spark.read.parquet(fileName)
  }

  def getCountOfPhiladelphiaRecords(): Long = {
    // implement the code
    ???
  }

  def getCountOfLARecords(): Long = {
    // implement the code
    ???
  }

  def getCountOfDallasRecords(): Long = {
    // implement the code
    ???
  }

  def getRobberyCountInLa(): Long = {
    // implement the code
    ???
  }

  def getRobberyCountInPhiladelphia(): Long = {
    // implement the code
    ???
  }

  def getRobberyCountInDallas(): Long = {
    // implement the code
    ???
  }

  def getRobberyDFByMonthInLa(): DataFrame = {
    // implement the code
    ???
  }

  def getRobberyDFByMonthInPhiladelphia(): DataFrame = {
    // implement the code
    ???
  }

  def getRobberyDFByMonthInDallas(): DataFrame = {
    // implement the code
    ???
  }

  def getCombinedRobberyDFByMonth(): DataFrame = {
    // implement the code
    ???
  }

  def getRobberyRatesByCity(): DataFrame = {
    // implement the code
    ???
  }
}
