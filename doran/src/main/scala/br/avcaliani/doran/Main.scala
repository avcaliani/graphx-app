package br.avcaliani.doran

import br.avcaliani.doran.utils.DataMock
import br.avcaliani.doran.utils.MyEdge
import br.avcaliani.doran.utils.MyVertex
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

object Main extends App {

  val spark = SparkSession
    .builder()
    .appName("doran")
    .config("spark.master", "local[*]")
    .getOrCreate()

  import spark.implicits._

  val mock = new DataMock(spark)

  // Create the graph
  val graph: Graph[MyVertex, MyEdge] = Graph(
    mock.items(), /* vertices */ 
    mock.itemsRelation() /* edges */
  )

  println("--- Vertices ---")
  graph.vertices.map(v => v._2).toDF().show()

  println("--- Edges ---")
  graph.edges.toDF().show()

}
