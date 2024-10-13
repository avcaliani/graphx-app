package br.avcaliani.doran

import br.avcaliani.doran.utils.DataMock
import org.apache.spark.sql.SparkSession

object Main extends App {

  val spark = SparkSession
    .builder()
    .appName("doran")
    .config("spark.master", "local")
    .getOrCreate()

  println("Hello, Doran!")
  new DataMock(spark).items().show()
}
