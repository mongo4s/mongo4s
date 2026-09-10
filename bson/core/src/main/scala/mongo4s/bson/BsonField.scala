package mongo4s.bson

/** Document keys MongoDB reserves, as they appear on the wire. */
object BsonField:

  /** The primary key every document carries.
    *
    * A [[FieldNaming]] never touches it: the server owns the name, so it is the one key that is written exactly as spelled here whatever convention the rest of the
    * document follows.
    */
  val Id: String = "_id"
