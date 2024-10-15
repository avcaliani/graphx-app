package br.avcaliani.doran.utils

import org.apache.spark.graphx.VertexId

// VertexId is the same as Long.
// This represents a vertex, however graphx requires
// a RDD of Tuple(VertexId, Any) and in this case Any is the MyVertex. 
case class MyVertex(id: VertexId, name: String, rarity: String, cost: Int)

// This is my custom class to represent the edges value.
// The GraphX implementation requires an RDD of Edge.
// An Edge object must have src, dest, value (MyEdge for example).
case class MyEdge(amount: Int)
