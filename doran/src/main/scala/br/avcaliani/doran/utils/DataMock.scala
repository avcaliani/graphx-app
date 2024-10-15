package br.avcaliani.doran.utils

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

// This object is a mock where I have some data to test my application.
// For real use cases you should read from a file, topic, database, etc...
class DataMock(spark: SparkSession) {

  import spark.implicits._

  private val itemsData = Seq(
    MyVertex(1L, "Ruby Crystal", "Basic", 400),
    MyVertex(2L, "Bami's Cinder", "Epic", 200),
    MyVertex(3L, "Cloth Armor", "Basic", 300),
    MyVertex(4L, "Chain Vest", "Epic", 500),
    MyVertex(5L, "Sunfire Aegis", "Legendary", 900)
  )

  private val itemsRelationData = Seq(
    (1L, 2L, 2),
    (3L, 4L, 1),
    (2L, 5L, 1),
    (4L, 5L, 1)
  )

  // Why am I converting the Seq to DataFrame and then to RDD?
  // Because in a real case, you probably will have a DataFrame and not an RDD.
  def items(): RDD[(VertexId, MyVertex)] = {
    this.itemsData
      .toDF("id", "name", "rarity", "cost")
      .as[MyVertex]
      .rdd
      .map(v => (v.id, v))
  }

  // Here I did the same, but assuming your data frame doesn't
  // have a case class.
  def itemsRelation(): RDD[Edge[MyEdge]] = {
    this.itemsRelationData
      .toDF("src", "dst", "amount")
      .rdd
      .map(row =>
        Edge(
          srcId = row.getLong(0),
          dstId = row.getLong(1),
          attr = MyEdge(row.getInt(2))
        )
      )
  }

}
