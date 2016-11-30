package recorder

import java.io._
import java.sql.Connection
import java.time.{Duration, Instant}

import model.Trajectory
import model.Trajectory._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.WriteResult
import scalikejdbc._

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global

object TrajectoryUtils {

  def persistByMongoDb(collection : BSONCollection, fileLimit: Range, resources: Array[File]){
    val begin = Instant.now()
    fileLimit foreach { i =>
      trajectories(resources(i)) foreach { insertTrajectory(collection, _) }
    }
    val end = Instant.now()
    println("------------------------")
    println(s"Duration for inserting all rows (MongoDB): ${Duration.between(begin, end)}")
    println("------------------------")
  }

  def persistBySql(conn: Connection, fileLimit: Range, resources: Array[File]) {
    val begin = Instant.now()
    fileLimit foreach { i =>
      trajectories(resources(i)) foreach { insertTrajectory(conn, _) }
    }
    val end = Instant.now()
    println("------------------------")
    println(s"Duration for inserting all rows (MySQL): ${Duration.between(begin, end)}")
    println("------------------------")
  }

  private def trajectories(file: File) = fileLines(file).map { line => createTrajectory(line) }

  private def fileLines(source: File) = Source.fromFile(source).getLines

  private def createTrajectory(line: String) = Trajectory(line split(","))

  private def insertTrajectory(conn: Connection, t : Trajectory) = {
    val statement = conn.prepareStatement(s"insert into trajectories(id_taxi, when_, latitude, longitude) " +
      s"values (${t.idTaxi}, ?, ${t.latitude}, ${t.longitude})")
    statement.setString(1, t.when)
    statement.executeUpdate()
    statement.close()
  }

  private def insertTrajectory(collection: BSONCollection, tr: Trajectory)(implicit exc : ExecutionContext) : Future[WriteResult] =
    collection.insert(tr)
}