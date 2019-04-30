package crimes
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

trait SparkTest extends QueryTest with SharedSQLContext{
  override protected def initializeSession(): Unit = {
    super.initializeSession()
    spark.sparkContext.setLogLevel(org.apache.log4j.Level.ERROR.toString)
  }
}
