package org.neo4j.spark

import org.apache.spark.sql.types
import org.apache.spark.sql.types.{StructField, StructType}

object CypherTypes {
  val INTEGER = types.LongType
  val FlOAT = types.DoubleType
  val STRING = types.StringType
  val BOOLEAN = types.BooleanType
  val NULL = types.NullType

  def apply(typ:String) = typ.toUpperCase match {
    case "LONG" => INTEGER
    case "INT" => INTEGER
    case "INTEGER" => INTEGER
    case "FLOAT" => FlOAT
    case "DOUBLE" => FlOAT
    case "NUMERIC" => FlOAT
    case "STRING" => STRING
    case "BOOLEAN" => BOOLEAN
    case "BOOL" => BOOLEAN
    case "NULL" => NULL
    case _ => STRING
  }
//  val MAP = edges.MapType(edges.StringType,edges.AnyDataType)
//  val LIST = edges.ArrayType(edges.AnyDataType)
// , MAP, LIST, NODE, RELATIONSHIP, PATH


  def toSparkType(typ : Class[_]): org.apache.spark.sql.types.DataType = {
    if (typ == classOf[Boolean]) CypherTypes.BOOLEAN
    if (typ == classOf[String]) CypherTypes.STRING
    if (typ == classOf[Int] || typ == classOf[Double]) CypherTypes.INTEGER
    if (typ == classOf[Float] || typ == classOf[Double]) CypherTypes.FlOAT
    // edges.MapType(edges.StringType,edges.ObjectType)
    // typeSystem.NODE => edges.VertexType
    // typeSystem.PATH => edges.GraphType
    CypherTypes.STRING
  }

  def field(keyType: (String, Class[_])): StructField = {
    StructField(keyType._1, CypherTypes.toSparkType(keyType._2))
  }
  def schemaFromNamedType(schemaInfo: Seq[(String, String)]) = {
    val fields = schemaInfo.map(field =>
      StructField(field._1, CypherTypes(field._2), nullable = true) )
    StructType(fields)
  }
  def schemaFromDataType(schemaInfo: Seq[(String, types.DataType)]) = {
    val fields = schemaInfo.map(field =>
      StructField(field._1, field._2, nullable = true) )
    StructType(fields)
  }

}
