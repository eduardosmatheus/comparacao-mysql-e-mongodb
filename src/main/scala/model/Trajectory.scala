package model

import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson
import reactivemongo.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}
import scalikejdbc._

import scala.concurrent.ExecutionContext


object Trajectory {
  def apply(a: Array[String]): Trajectory =
    new Trajectory(
      a(0).toInt,
      a(1).toString,
      a(2).toString,
      a(3).toString
    )

  def findByCoordinates(latitude: String, longitude: String)(implicit session: AutoSession, db: DB) : Int =
    sql"SELECT * from trajectories WHERE latitude = ${latitude} and longitude = ${longitude}"
      .map(rs => Trajectory(rs.int("id_taxi"), rs.string("when_"), rs.string("latitude"), rs.string("longitude")))
      .list().apply().size

  def findByCoordinates(collection: BSONCollection, latitude: String, longitude: String)
                       (implicit exc: ExecutionContext) = {
    println
    collection.find(bson.document(
      "latitude" -> BSONDocument("$eq" -> latitude),
      "longitude" -> BSONDocument("$eq" -> longitude)
    )).cursor[Trajectory].collect[List]()
  }

  implicit val TrajectoryWriter = new BSONDocumentWriter[Trajectory] {
    override def write(t: Trajectory): BSONDocument =
      BSONDocument(
        "id_taxi" -> t.idTaxi,
        "when" -> t.when,
        "latitude" -> t.latitude,
        "longitude" -> t.longitude
      )
  }

  implicit val TrajectoryReader = new BSONDocumentReader[Trajectory] {
    override def read(doc: BSONDocument): Trajectory =
      new Trajectory(
        doc.getAs[Int]("id_taxi").get,
        doc.getAs[String]("when").get,
        doc.getAs[String]("latitude").get,
        doc.getAs[String]("longitude").get
      )
  }
}

case class Trajectory(idTaxi: Int, when: String, latitude: String, longitude: String)