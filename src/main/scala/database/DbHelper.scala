package database

import java.sql.DriverManager

import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}

import scala.concurrent.{ExecutionContext, Future}

object MySqlHelper {

  val mysqlConnection = {
    Class forName("com.mysql.cj.jdbc.Driver")
    val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/taxi_test", "root", "")
    connection.setAutoCommit(false)
    connection
  }
}
object MongoHelper {

  private val driver = MongoDriver()

  private val parsedUrl = MongoConnection.parseURI("mongodb://localhost:27017/my_db")

  private val connection = parsedUrl.map(x => driver.connection(x))

  private val futureConnection = Future.fromTry(connection)

  private def myDb(implicit executionContext : ExecutionContext) : Future[DefaultDB] =
    futureConnection.flatMap(_.database("my_db"))

  def taxiCollection(implicit executionContext: ExecutionContext) : Future[BSONCollection] =
    myDb.map(_.collection("trajectories"))
}