package mongo4s.bson.direct

import scala.compiletime.uninitialized

import org.bson.*
import org.bson.types.{Decimal128, ObjectId}

private[direct] final class BsonValueCapturingWriter extends AbstractBsonWriter(BsonWriterSettings()):
  setContext(new Context(null, BsonContextType.TOP_LEVEL))
  setState(AbstractBsonWriter.State.VALUE)

  private var captured: BsonValue = uninitialized

  def result: BsonValue =
    if captured == null
    then throw IllegalStateException("ScalarWireCodec.toBsonEncoder: the codec wrote nothing")
    else captured

  private def capture(value: BsonValue): Unit = captured = value

  private def unsupported(shape: String): Nothing =
    throw UnsupportedOperationException(
      s"ScalarWireCodec.toBsonEncoder only supports a codec that writes a single scalar value directly " +
        s"(no nested $shape) — this codec tried to write one, which should be impossible for a genuine ScalarWireCodec."
    )

  protected def doWriteStartDocument(): Unit = unsupported("document")
  protected def doWriteEndDocument(): Unit   = unsupported("document")
  protected def doWriteStartArray(): Unit    = unsupported("array")
  protected def doWriteEndArray(): Unit      = unsupported("array")

  protected def doWriteBinaryData(value: BsonBinary): Unit                   = capture(value)
  protected def doWriteBoolean(value: Boolean): Unit                         = capture(BsonBoolean.valueOf(value))
  protected def doWriteDateTime(value: Long): Unit                           = capture(BsonDateTime(value))
  protected def doWriteDBPointer(value: BsonDbPointer): Unit                 = capture(value)
  protected def doWriteDouble(value: Double): Unit                           = capture(BsonDouble(value))
  protected def doWriteInt32(value: Int): Unit                               = capture(BsonInt32(value))
  protected def doWriteInt64(value: Long): Unit                              = capture(BsonInt64(value))
  protected def doWriteDecimal128(value: Decimal128): Unit                   = capture(BsonDecimal128(value))
  protected def doWriteJavaScript(value: String): Unit                       = capture(BsonJavaScript(value))
  protected def doWriteJavaScriptWithScope(value: String): Unit              = unsupported("javascript-with-scope")
  protected def doWriteMaxKey(): Unit                                        = capture(BsonMaxKey())
  protected def doWriteMinKey(): Unit                                        = capture(BsonMinKey())
  protected def doWriteNull(): Unit                                          = capture(BsonNull.VALUE)
  protected def doWriteObjectId(value: ObjectId): Unit                       = capture(BsonObjectId(value))
  protected def doWriteRegularExpression(value: BsonRegularExpression): Unit = capture(value)
  protected def doWriteString(value: String): Unit                           = capture(BsonString(value))
  protected def doWriteSymbol(value: String): Unit                           = capture(BsonSymbol(value))
  protected def doWriteTimestamp(value: BsonTimestamp): Unit                 = capture(value)
  protected def doWriteUndefined(): Unit                                     = capture(BsonUndefined())
