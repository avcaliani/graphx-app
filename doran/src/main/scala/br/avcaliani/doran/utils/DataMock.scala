package br.avcaliani.doran.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/** This object is a mock where I have some data to test my application. For real use cases you
  * should read from a file, topic, database, etc...
  */
class DataMock(spark: SparkSession) {

  import spark.implicits._

  private val itemsData = Seq(
    ("Ruby Crystal", "Basic", 400),
    ("Bami's Cinder", "Epic", 200),
    ("Cloth Armor", "Basic", 300),
    ("Chain Vest", "Epic", 500),
    ("Sunfire Aegis", "Legendary", 900)
  )

  private val itemsRelationData = Seq(
    ("Ruby Crystal", "Bami's Cinder", 2),
    ("Cloth Armor", "Chain Vest", 1),
    ("Bami's Cinder", "Sunfire Aegis", 1),
    ("Chain Vest", "Sunfire Aegis", 1)
  )

  def items(): DataFrame = {
    this.itemsData
      .toDF("id", "rarity", "cost")
  }

  def itemsRelation(): DataFrame = {
    this.itemsRelationData
      .toDF("src", "dst", "amount")
  }

}
