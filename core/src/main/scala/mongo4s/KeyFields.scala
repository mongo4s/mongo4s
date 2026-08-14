package mongo4s

import org.bson.BsonValue

final case class KeyFields(head: (String, BsonValue), tail: List[(String, BsonValue)]):
  def toList: List[(String, BsonValue)] = head :: tail
  def size: Int                         = tail.size + 1

object KeyFields:
  def one(name: String, value: BsonValue): KeyFields                       = KeyFields(name -> value, Nil)
  def of(head: (String, BsonValue), tail: (String, BsonValue)*): KeyFields = KeyFields(head, tail.toList)
