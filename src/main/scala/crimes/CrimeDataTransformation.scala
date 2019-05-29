package crimes

import org.apache.spark.sql.functions.{col, lit, lower, month}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CrimeDataTransformation {
  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
  }
  spark.conf.set("spark.sql.session.timeZone", "UTC")

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

  private val laRobberyDF: DataFrame = {
    laDF.filter(lower(col("crimeCodeDescription")) === "robbery")
  }

  private val philadelphiaRobberyDF: DataFrame = {
    philadelphiaDF.filter(lower(col("ucr_general_description")) === "robbery")
  }

  private val dallasRobberyDF: DataFrame = {
    dallasDF.filter(
      lower(col("typeOfIncident")).contains("robbery")
        &&
        !lower(col("typeOfIncident")).contains("burglary")
    )
  }

  private val robberiesByMonthLADF: DataFrame = {
    laRobberyDF
      .groupBy(month(col("timeOccurred")) as "month")
      .count
      .toDF("month", "robberies")
      .orderBy("month")
  }

  private val robberiesByMonthPhiladelphiaDF: DataFrame = {
    philadelphiaRobberyDF
      .groupBy(month(col("dispatch_date_time")) as "month")
      .count
      .toDF("month", "robberies")
      .orderBy("month")
  }

  private val robberiesByMonthDallasDF: DataFrame = {
    dallasRobberyDF
      .groupBy(month(col("startingDateTime")) as "month")
      .count
      .toDF("month", "robberies")
      .orderBy("month")
  }

  private val combinedRobberiesByMonthDF: DataFrame = {
    robberiesByMonthLADF
      .withColumn("city", lit("Los Angeles"))
      .union(
        robberiesByMonthPhiladelphiaDF.withColumn("city", lit("Philadelphia"))
      )
      .union(
        robberiesByMonthDallasDF.withColumn("city", lit("Dallas"))
      )
      .select(col("city"), col("month"), col("robberies"))
  }

  private val robberyRatesByCityDF: DataFrame = {
    val fileName = "./src/test/resources/CityData.parquet"
    val cityDataDF = spark.read.parquet(fileName).withColumnRenamed("city", "cities")
    import org.apache.spark.sql.functions.format_number
    combinedRobberiesByMonthDF
      .join(cityDataDF, col("city") === col("cities"))
      .withColumn("robberyRate", format_number(col("robberies") / col("estPopulation2016"), 6))
      .select(col("city"), col("month"), col("robberyRate"))
  }

  def getCountOfPhiladelphiaRecords(): Long = {
    philadelphiaDF.count()
  }

  def getCountOfLARecords(): Long = {
    laDF.count()
  }

  def getCountOfDallasRecords(): Long = {
    dallasDF.count()
  }

  def getRobberyCountInLa(): Long = {
    laRobberyDF.count()
  }

  def getRobberyCountInPhiladelphia(): Long = {
    philadelphiaRobberyDF.count()
  }

  def getRobberyCountInDallas(): Long = {
    dallasRobberyDF.count()
  }

  def getRobberyDFByMonthInLa(): DataFrame = {
    robberiesByMonthLADF
  }

  def getRobberyDFByMonthInPhiladelphia(): DataFrame = {
    robberiesByMonthPhiladelphiaDF
  }

  def getRobberyDFByMonthInDallas(): DataFrame = {
    robberiesByMonthDallasDF
  }

  def getCombinedRobberyDFByMonth(): DataFrame = {
    combinedRobberiesByMonthDF
  }

  def getRobberyRatesByCity(): DataFrame = {
    robberyRatesByCityDF
  }
}
