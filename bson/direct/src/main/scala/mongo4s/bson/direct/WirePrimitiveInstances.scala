package mongo4s.bson.direct

trait WirePrimitiveInstances extends WireFallbackInstances:

  given ScalarWireCodec[String]  = ScalarWireCodec.instance((w, v) => w.writeString(v), r => r.readString())
  given ScalarWireCodec[Int]     = ScalarWireCodec.instance((w, v) => w.writeInt32(v), r => r.readInt32())
  given ScalarWireCodec[Long]    = ScalarWireCodec.instance((w, v) => w.writeInt64(v), r => r.readInt64())
  given ScalarWireCodec[Double]  = ScalarWireCodec.instance((w, v) => w.writeDouble(v), r => r.readDouble())
  given ScalarWireCodec[Boolean] = ScalarWireCodec.instance((w, v) => w.writeBoolean(v), r => r.readBoolean())
