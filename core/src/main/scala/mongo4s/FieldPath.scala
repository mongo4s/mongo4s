package mongo4s

import scala.annotation.targetName

import mongo4s.bson.FieldNaming

final case class FieldPath(segments: List[String], literal: Boolean = false):
  def render(naming: FieldNaming): String =
    if literal
    then segments.mkString(".")
    else segments.map(naming.apply).mkString(".")

  @targetName("child")
  infix def /(segment: String): FieldPath = copy(segments = segments :+ segment)

object FieldPath:
  def of(segment: String): FieldPath     = FieldPath(List(segment))
  def literal(stored: String): FieldPath = FieldPath(stored.split('.').toList, literal = true)
