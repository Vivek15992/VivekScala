package com.spark.learn
import org.apache.spark._
import org.apache.spark.sql.functions.{ max, broadcast, col,lit,udf }

import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.log4j._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
object GrapxEx2 {
  case class Flight(dofM:String, dofW:String, carrier:String, 
    tailnum:String, flnum:Int, org_id:Long, origin:String, 
    dest_id:Long, dest:String, crsdeptime:Double, deptime:Double, 
    depdelaymins:Double, crsarrtime:Double, arrtime:Double, 
    arrdelay:Double,crselapsedtime:Double,dist:Int)
 
def parseFlight(str: String): Flight = {
  val line = str.split(",")
  Flight(line(0), line(1), line(2), line(3), line(4).toInt, 
      line(5).toLong, line(6), line(7).toLong, line(8), 
      line(9).toDouble, line(10).toDouble, line(11).toDouble,
      line(12).toDouble, line(13).toDouble, line(14).toDouble,
      line(15).toDouble, line(16).toInt)
}

  def main(args: Array[String]) {

    // Use new SparkSession interface in Spark 2.0
    val sc = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
  //Create RDD with the January 2014 data 
val textRDD = sc.sparkContext.textFile("C:/Users/hp/workspace/rita2014jan.csv")

val flightsRDD = textRDD.map(parseFlight).cache()

val airports = flightsRDD.map(flight => (flight.org_id, flight.origin)).distinct    
    airports.take(1)
//  Array((14057,PDX))

// Defining a default vertex called nowhere
val nowhere = "nowhere"

val routes = flightsRDD.map(flight => ((flight.org_id, flight.dest_id), flight.dist)).distinct

// Array(((14869,14683),1087), ((14683,14771),1482)) 
routes.cache

val airportMap = airports.map { case ((org_id), name) => (org_id -> name) }.collect.toList.toMap

//airportMap: scala.collection.immutable.Map[Long,String] = Map(13024 -> LMT, 10785 -> BTV, 14574 -> ROA, 14057 -> PDX, 13933 -> ORH, 11898 -> GFK, 14709 -> SCC, 15380 -> TVC,
    
// Defining the routes as edges
val edges = routes.map { case ((org_id, dest_id), distance) => Edge(org_id.toLong, dest_id.toLong, distance) }

edges.take(1)
//res80: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(10299,10926,160))

//Defining the Graph
val graph = Graph(airports, edges, nowhere)

// LNumber of airports   
val numairports = graph.numVertices
// numairports: Long = 301
graph.vertices.take(2)

graph.edges.take(2)
// res6: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(10135,10397,692), Edge(10135,13930,654))

// which routes >  1000 miles distance?
 graph.edges.filter { case ( Edge(org_id, dest_id,distance))=> distance > 1000}.take(3)
// res9: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(10140,10397,1269), Edge(10140,10821,1670), Edge(10140,12264,1628))

// Number of routes
val numroutes = graph.numEdges
// numroutes: Long = 4090

// The EdgeTriplet class extends the Edge class by adding the srcAttr and dstAttr members which contain the source and destination properties respectively.   
graph.triplets.take(3).foreach(println)
/*((10135,ABE),(10397,ATL),692)
((10135,ABE),(13930,ORD),654)
((10140,ABQ),(10397,ATL),1269)
*/
//Sort and print out the longest distance routes
graph.triplets.sortBy(_.attr, ascending=false).map(triplet =>
         "Distance " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10).foreach(println)

/*Distance 4983 from JFK to HNL.
Distance 4983 from HNL to JFK.
Distance 4963 from EWR to HNL.
Distance 4963 from HNL to EWR.
Distance 4817 from HNL to IAD.
Distance 4817 from IAD to HNL.
Distance 4502 from ATL to HNL.
Distance 4502 from HNL to ATL.
Distance 4243 from HNL to ORD.
Distance 4243 from ORD to HNL.*/

// Compute the max degrees
val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
// maxInDegree: (org.apache.spark.graphx.VertexId, Int) = (10397,152)
val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)

println("maxInDegree")
println(maxInDegree._1+","+maxInDegree._2)

println("maxOutDegree")
println(maxOutDegree._1+","+maxOutDegree._2)
  }
  // Define a reduce operation to compute the highest degree vertex
def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 > b._2) a else b
}
}