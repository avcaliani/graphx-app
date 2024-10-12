package br.avcaliani.doran

import org.apache.spark.sql.SparkSession
import br.avcaliani.doran.utils.DataMock

object Main extends App {

  val spark = SparkSession.builder()
    .appName("doran")
    .config("spark.master", "local")
    .getOrCreate()

  println("Hello, Doran!")
  new DataMock(spark).items().show()
}
