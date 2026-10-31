package mongo4s.bson

trait BsonCodecSource[A]:
  def toBsonEncoder: BsonEncoder[A]
  def toBsonDecoder: BsonDecoder[A]
