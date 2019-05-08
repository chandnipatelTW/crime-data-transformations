package crimes

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.{QueryTest }

class CrimeDataTransformationTest extends QueryTest with SharedSQLContext{

  import testImplicits._

  test("should return total count of philadelphia records") {
    val philadelphiaCount = CrimeDataTransformation.getCountOfPhiladelphiaRecords()
    assert(philadelphiaCount == 168664)
  }

  test("should return total count of Dallas' crime records") {
    val dallasCount = CrimeDataTransformation.getCountOfDallasRecords()
    assert(dallasCount == 99642)
  }

  test("should return total count of crime records in Los Angeles") {
    val laCount = CrimeDataTransformation.getCountOfLARecords()
    assert(laCount == 217945)
  }

//  Explore the data for the three cities until you understand how each city records robbery information. You need to consider only robbery

  test("should return total robbery count in Los Angeles") {
    assert(CrimeDataTransformation.getRobberyCountInLa() == 9048)
  }

  test("should return total robbery count in Philadelphia") {
    assert(CrimeDataTransformation.getRobberyCountInPhiladelphia() == 6149)
  }

  test("should return total robbery count in Dallas") {
    assert(CrimeDataTransformation.getRobberyCountInDallas()  == 6824)
  }

//  Your dataframes must contain 2 columns: month and robberies and use column name:timeOccurred to get the data
  test("should return the number of robberies in each month in LA") {
//    Hint : Consider using UTC timezone
    val actualNoOfRobberiesInLAPerMonth = CrimeDataTransformation.getRobberyDFByMonthInLa()
    val expectedNoOfRobberiesInLAPerMonth = Seq((1, 719), (2, 675), (3, 709), (4, 713), (5, 790), (6, 698), (7, 826), (8, 765), (9, 722), (10, 814), (11, 764), (12, 853)).toDF()

    checkAnswer(actualNoOfRobberiesInLAPerMonth, expectedNoOfRobberiesInLAPerMonth)
  }

  //  Your dataframes must contain 2 columns: month and robberies and use column name:dispatch_date_time to get the data
  test("should return the number of robberies in each month in Philadelphia") {
    //    Hint : Consider using UTC timezone
    val actualNoOfRobberiesInPhilPerMonth = CrimeDataTransformation.getRobberyDFByMonthInPhiladelphia()
    val expectedNoOfRobberiesInPhilPerMonth = Seq((1, 520), (2, 416), (3, 432), (4, 466), (5, 533), (6, 509), (7, 537), (8, 561), (9, 514), (10, 572), (11, 545), (12, 544)).toDF()

    checkAnswer(actualNoOfRobberiesInPhilPerMonth, expectedNoOfRobberiesInPhilPerMonth)
  }

  //  Your dataframes must contain 2 columns: month and robberies and use column name:startingDateTime to get the data
  test("should return the number of robberies in each month in Dallas") {
    //    Hint : Consider using UTC timezone
    val actualNoOfRobberiesInDallasPerMonth = CrimeDataTransformation.getRobberyDFByMonthInDallas()
    val expectedNoOfRobberiesInDallasPerMonth = Seq((1, 743), (2, 435), (3, 412), (4, 594), (5, 615), (6, 495), (7, 535), (8, 627), (9, 512), (10, 603), (11, 589), (12, 664)).toDF()

    checkAnswer(actualNoOfRobberiesInDallasPerMonth, expectedNoOfRobberiesInDallasPerMonth)
  }

  // Your dataframes should contain 3 columns: city, month and robbery
  test("should return the combined number of robberies in each month for each city") {
    val ActualCombinedNumberOfRobberiesPerMonth = CrimeDataTransformation.getCombinedRobberyDFByMonth()
    val expectedCombinedNumberOfRobberiesPerMonth = Seq(("Dallas", 11, 589), ("Los Angeles", 2, 675), ("Dallas", 8, 627), ("Los Angeles", 9, 722), ("Los Angeles", 1, 719), ("Philadelphia", 12, 544), ("Dallas", 1, 743), ("Dallas", 10, 603), ("Dallas", 6, 495), ("Los Angeles", 4, 713), ("Philadelphia", 2, 416), ("Dallas", 4, 594), ("Los Angeles", 12, 853), ("Dallas", 12, 664), ("Dallas", 9, 512), ("Los Angeles", 3, 709), ("Dallas", 2, 435), ("Los Angeles", 7, 826), ("Philadelphia", 1, 520), ("Los Angeles", 5, 790), ("Philadelphia", 7, 537), ("Dallas", 5, 615), ("Philadelphia", 9, 514), ("Los Angeles", 6, 698), ("Philadelphia", 8, 561), ("Los Angeles", 11, 764), ("Philadelphia", 6, 509), ("Dallas", 3, 412), ("Philadelphia", 5, 533), ("Philadelphia", 10, 572), ("Los Angeles", 10, 814), ("Los Angeles", 8, 765), ("Philadelphia", 11, 545), ("Dallas", 7, 535), ("Philadelphia", 3, 432), ("Philadelphia", 4, 466)).toDF()

    checkAnswer(ActualCombinedNumberOfRobberiesPerMonth, expectedCombinedNumberOfRobberiesPerMonth)
  }

  test("should return the robbery rates by city") {
    val actualRobberyRatesByCity = CrimeDataTransformation.getRobberyRatesByCity()
    val expectedRobberyRatesByCity = Seq(
      ("Dallas",  1, "0.000564"),
      ("Dallas",  2, "0.000330"),
      ("Dallas",  3, "0.000313"),
      ("Dallas",  4, "0.000451"),
      ("Dallas",  5, "0.000467"),
      ("Dallas",  6, "0.000376"),
      ("Dallas",  7, "0.000406"),
      ("Dallas",  8, "0.000476"),
      ("Dallas",  9, "0.000388"),
      ("Dallas", 10, "0.000458"),
      ("Dallas", 11, "0.000447"),
      ("Dallas", 12, "0.000504"),
      ("Los Angeles",  1, "0.000181"),
      ("Los Angeles",  2, "0.000170"),
      ("Los Angeles",  3, "0.000178"),
      ("Los Angeles",  4, "0.000179"),
      ("Los Angeles",  5, "0.000199"),
      ("Los Angeles",  6, "0.000176"),
      ("Los Angeles",  7, "0.000208"),
      ("Los Angeles",  8, "0.000192"),
      ("Los Angeles",  9, "0.000182"),
      ("Los Angeles", 10, "0.000205"),
      ("Los Angeles", 11, "0.000192"),
      ("Los Angeles", 12, "0.000215"),
      ("Philadelphia",  1, "0.000332"),
      ("Philadelphia",  2, "0.000265"),
      ("Philadelphia",  3, "0.000276"),
      ("Philadelphia",  4, "0.000297"),
      ("Philadelphia",  5, "0.000340"),
      ("Philadelphia",  6, "0.000325"),
      ("Philadelphia",  7, "0.000343"),
      ("Philadelphia",  8, "0.000358"),
      ("Philadelphia",  9, "0.000328"),
      ("Philadelphia", 10, "0.000365"),
      ("Philadelphia", 11, "0.000348"),
      ("Philadelphia", 12, "0.000347")).toDF()

    checkAnswer(actualRobberyRatesByCity, expectedRobberyRatesByCity)
  }
}
