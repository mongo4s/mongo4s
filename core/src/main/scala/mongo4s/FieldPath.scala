package mongo4s

import scala.annotation.targetName

import mongo4s.bson.FieldNaming

opaque type FieldPath = List[FieldPath.Segment]

object FieldPath:

  enum Segment:
    case Derived(name: String)
    case Stored(name: String)

    def render(naming: FieldNaming): String =
      this match
        case Derived(name) => naming.apply(name)
        case Stored(name)  => name

  def apply(segments: List[Segment]): FieldPath = segments

  /** The reserved primary-key field. Never renamed, whatever the `FieldNaming`. */
  val IdName: String = "_id"

  def of(segment: String): FieldPath             = List(Segment.Derived(segment))
  def derived(segments: List[String]): FieldPath = segments.map(Segment.Derived.apply)
  def literal(stored: String): FieldPath         = stored.split('.').toList.map(Segment.Stored.apply)

  val Id: FieldPath = literal(IdName)

  extension (path: FieldPath)

    def render(naming: FieldNaming): String =
      path match
        case single :: Nil => single.render(naming)
        case many          => many.iterator.map(_.render(naming)).mkString(".")

    def stored(segment: String): FieldPath = path :+ Segment.Stored(segment)

    @targetName("child")
    infix def /(segment: String): FieldPath = path.stored(segment)
