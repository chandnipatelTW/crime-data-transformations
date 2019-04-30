package main
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

class CrimeDataTransformationTest extends TestHelper {
  import spark.implicits._

  lazy val laDF: DataFrame = {
    val fileName = "./src/test/resources/LA.parquet"
    spark.read.parquet(fileName)
  }

  lazy val philadelphiaDF: DataFrame = {
    val fileName = "./src/test/resources/Philadelphia.parquet"
    spark.read.parquet(fileName)
  }

  lazy val dallasDF: DataFrame = {
    val fileName = "./src/test/resources/Dallas.parquet"
    spark.read.parquet(fileName)
  }

//  lazy val laRobberyDF: DataFrame = {
//    laDF.filter(lower($"crimeCodeDescription") === "robbery")
//  }
//
//  lazy val philadelphiaRobberyDF: DataFrame = {
//    philadelphiaDF.filter(lower($"ucr_general_description") === "robbery")
//  }
//
//  lazy val dallasRobberyDF: DataFrame = {
//    dallasDF.filter(lower($"typeOfIncident").contains("robbery") && !lower($"typeOfIncident").contains("burglary"))
//  }
//
//  lazy val robberiesByMonthLADF: DataFrame = {
//    laRobberyDF.groupBy(month($"timeOccurred") as "month").count.toDF("month","robberies").orderBy("month")
//  }
//
//  lazy val robberiesByMonthPhiladelphiaDF: DataFrame = {
//    philadelphiaRobberyDF.groupBy(month($"dispatch_date_time") as "month").count.toDF("month","robberies").orderBy("month")
//  }
//
//  lazy val robberiesByMonthDallasDF: DataFrame = {
//    dallasRobberyDF.groupBy(month($"startingDateTime") as "month").count.toDF("month","robberies").orderBy("month")
//  }
//
//  lazy val combinedRobberiesByMonthDF: DataFrame  = {
//    robberiesByMonthLADF
//      .withColumn("city", lit("Los Angeles"))
//      .union(
//        robberiesByMonthPhiladelphiaDF.withColumn("city", lit("Philadelphia"))
//      )
//      .union(
//        robberiesByMonthDallasDF.withColumn("city", lit("Dallas"))
//      )
//      .select($"city", $"month", $"robberies")
//  }
//
//  lazy val robberyRatesByCityDF: DataFrame  = {
//    val fileName = "./src/test/resources/CityData.parquet"
//    val cityDataDF = spark.read.parquet(fileName).withColumnRenamed("city", "cities")
//    import org.apache.spark.sql.functions.format_number
//    combinedRobberiesByMonthDF
//      .join(cityDataDF, $"city" === $"cities")
//      .withColumn("robberyRate", format_number($"robberies" / $"estPopulation2016", 6))
//      .select($"city", $"month", $"robberyRate")
//  }

  test("should return total count of philadelphia records") {
    val totalRecords = 0
    assert( totalRecords == 168664)
  }

  test("should return total count of Dallas' crime records") {
    val totalDallasRecords = 0
    assert( totalDallasRecords == 99642)
  }

  test("should return total count of crime records in Los Angeles") {
    val totalLARecords = 0
    assert( totalLARecords == 217945)
  }

  test("should return total robbery count in Los Angeles") {
    val totalLARobberyDFCount = 0
    assert( totalLARobberyDFCount == 9048)
  }

  test("should return total robbery count in Philadelphia") {
    val totalPhiladelphiaRobberyDFCount = 0
    assert( totalPhiladelphiaRobberyDFCount == 6149)
  }

  test("should return total robbery count in Dallas") {
    val totalDallasRobberyDFCount = 0
    assert( totalDallasRobberyDFCount == 6824)
  }

  test("should return the number of robberies in each month in LA"){
    val la = List.empty
    assert(la == List(Row(1,719), Row(2,675), Row(3,709), Row(4,713), Row(5,790), Row(6,698), Row(7,826), Row(8,765), Row(9,722), Row(10,814), Row(11,764), Row(12,853)), "the robbery count is incorrect")
    println("Tests passed!")
  }

  test("should return the number of robberies in each month in Philadelphia"){
    val philadelphia  = List.empty
    assert(philadelphia == List(Row(1,520), Row(2,416), Row(3,432), Row(4,466), Row(5,533), Row(6,509), Row(7,537), Row(8,561), Row(9,514), Row(10,572), Row(11,545), Row(12,544)), "the robberies by month data is incorrect")
    println("Tests passed!")
  }
  test("should return the number of robberies in each month in Dallas"){
    val dallas  = List.empty
    assert(dallas == List(Row(1, 743), Row(2, 435), Row(3,412), Row(4,594), Row(5,615), Row(6,495), Row(7,535), Row(8,627), Row(9,512), Row(10,603), Row(11,589), Row(12,664)), "the robberies by month data is incorrect")
    println("Tests passed!")
  }

  test("should return the combined number of robberies in each month"){
    lazy val results = List.empty
    assert(Set(Row("Dallas",11,589), Row("Los Angeles",2,675), Row("Dallas",8,627), Row("Los Angeles",9,722), Row("Los Angeles",1,719), Row("Philadelphia",12,544), Row("Dallas",1,743), Row("Dallas",10,603), Row("Dallas",6,495), Row("Los Angeles",4,713), Row("Philadelphia",2,416), Row("Dallas",4,594), Row("Los Angeles",12,853), Row("Dallas",12,664), Row("Dallas",9,512), Row("Los Angeles",3,709), Row("Dallas",2,435), Row("Los Angeles",7,826), Row("Philadelphia",1,520), Row("Los Angeles",5,790), Row("Philadelphia",7,537), Row("Dallas",5,615), Row("Philadelphia",9,514), Row("Los Angeles",6,698), Row("Philadelphia",8,561), Row("Los Angeles",11,764), Row("Philadelphia",6,509), Row("Dallas",3,412), Row("Philadelphia",5,533), Row("Philadelphia",10,572), Row("Los Angeles",10,814), Row("Los Angeles",8,765), Row("Philadelphia",11,545), Row("Dallas",7,535), Row("Philadelphia",3,432), Row("Philadelphia",4,466)) == results, "the robberies by month data is incorrect")

    println("Tests passed!")
  }

  test("should return the robbery rates by city"){
    lazy val results = List.empty
    lazy val expectedResults = List(
      Row("Dallas",  1, "0.000564"),
      Row("Dallas",  2, "0.000330"),
      Row("Dallas",  3, "0.000313"),
      Row("Dallas",  4, "0.000451"),
      Row("Dallas",  5, "0.000467"),
      Row("Dallas",  6, "0.000376"),
      Row("Dallas",  7, "0.000406"),
      Row("Dallas",  8, "0.000476"),
      Row("Dallas",  9, "0.000388"),
      Row("Dallas", 10, "0.000458"),
      Row("Dallas", 11, "0.000447"),
      Row("Dallas", 12, "0.000504"),
      Row("Los Angeles",  1, "0.000181"),
      Row("Los Angeles",  2, "0.000170"),
      Row("Los Angeles",  3, "0.000178"),
      Row("Los Angeles",  4, "0.000179"),
      Row("Los Angeles",  5, "0.000199"),
      Row("Los Angeles",  6, "0.000176"),
      Row("Los Angeles",  7, "0.000208"),
      Row("Los Angeles",  8, "0.000192"),
      Row("Los Angeles",  9, "0.000182"),
      Row("Los Angeles", 10, "0.000205"),
      Row("Los Angeles", 11, "0.000192"),
      Row("Los Angeles", 12, "0.000215"),
      Row("Philadelphia",  1, "0.000332"),
      Row("Philadelphia",  2, "0.000265"),
      Row("Philadelphia",  3, "0.000276"),
      Row("Philadelphia",  4, "0.000297"),
      Row("Philadelphia",  5, "0.000340"),
      Row("Philadelphia",  6, "0.000325"),
      Row("Philadelphia",  7, "0.000343"),
      Row("Philadelphia",  8, "0.000358"),
      Row("Philadelphia",  9, "0.000328"),
      Row("Philadelphia", 10, "0.000365"),
      Row("Philadelphia", 11, "0.000348"),
      Row("Philadelphia", 12, "0.000347"))
    assert(expectedResults == results, "the robberies by city data is incorrect")

    println("Tests passed!")
  }
}
