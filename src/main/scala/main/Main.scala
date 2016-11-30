package main

import java.io.File
import java.sql.Connection
import java.time.{Duration, Instant}
import scalikejdbc._
import model.Trajectory._
import database.MongoHelper._
import database.MySqlHelper._
import model.Trajectory
import reactivemongo.bson.{BSONDocument, BSONDocumentWriter, BSONWriter}
import recorder.TrajectoryUtils._
import scalikejdbc.{AutoSession, ConnectionPool}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionContext

object Main extends App {

  override def main(args: Array[String]) {
    implicit val session = AutoSession
    initializeMySql

    println("**************MYSQL*************")
    runMySql(0 to 800)
    println("**************MongoDB*************")
    runMongoDb(0 to 800)

    mysqlConnection.close()
  }

  private def initializeMySql {
    Class forName("com.mysql.cj.jdbc.Driver")
    ConnectionPool singleton("jdbc:mysql://localhost:3306/taxi_test", "root", "")
  }

  private def runMySql(limit: Range)(implicit session: AutoSession = AutoSession) {
    persistBySql(mysqlConnection, limit, resources)
    mysqlConnection.commit()
    println(s"${limit.size} files inserted! Initializing MySQL searching...")
    println("----------------")
    searchFromMySql
  }

  private def searchFromMySql(implicit session : AutoSession = AutoSession) {
    using(ConnectionPool().borrow()) { conn : Connection =>
      implicit val db : DB = DB(conn)

      val beginning = Instant.now
      val trajectoriesFound = findByCoordinates("116.46708", "39.92589")
      val ending = Instant.now
      println("----------------")
      println("Search finished successfully!")
      println("----------------")
      println(s"${trajectoriesFound} trajectories found! Elapsed time: ${Duration.between(beginning, ending)}")
      println("----------------")
      println()
      println("Retrieving all rows from the database....")
      println("----------------")
      searchMany(global, session = AutoSession)
      db localTx { implicit session =>
        println("Cleaning rows....")
        sql"delete from trajectories".executeUpdate().apply()
      }
    }
  }

  private def runMongoDb(numberOfFiles: Range) {
    println("------------------------")
    println("Inserting Data in a MongoDB Database.....")
    taxiCollection map { x =>
      persistByMongoDb(x, numberOfFiles, resources)
      searchFromMongoDb
    }
    println(s"${numberOfFiles.size} files inserted! Initializing MongoDB searching...")
    taxiCollection.map(_.drop(false))
  }

  private def searchFromMongoDb(implicit executionContext: ExecutionContext) {
    val beginning = Instant.now
    taxiCollection map {
      findByCoordinates(_, "116.46708","39.92589") map { trajectories =>
        println("----------------")
        println("Search finished successfully!")
        println(s"${trajectories.size} trajectories found in MongoDB!")
        println("----------------")
      }
    }
    val ending = Instant.now
    println("----------------")
    println(s"Elapsed time: ${Duration.between(beginning, ending)}")
    println("------------------------")
    println("Now retrieving all documents from the collection, regardless of filters...")
    println("------------------------")
    import model.Trajectory._
    val secondBeginning = Instant.now()
    taxiCollection.map( collection => {
      collection.find(BSONDocument()).cursor[Trajectory].collect[List]()
    })
    val secondEnding = Instant.now()
    println(s"All rows retrieved! Elapsed time: ${Duration.between(secondBeginning, secondEnding)}")
  }

  private def searchMany(implicit executionContext: ExecutionContext, session : AutoSession): Unit = {
    using(ConnectionPool.borrow()) { c =>
      val begin = Instant.now()
      sql"select * from trajectories".map(x => Trajectory(x.int(1), x.string(2), x.string(3), x.string(4)))
        .list().apply()
      val end = Instant.now()
      println(s"Retrieved all trajectories! Elapsed time: ${Duration.between(begin, end)}")
    }
  }

  lazy val resources = new File("./resources").listFiles()
}
