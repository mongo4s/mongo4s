package mongo4s.bson.direct

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter}

object WideDerivationSpec:
  sealed trait Big derives WireCodec

  object Big:
    final case class C0(n0: Int)   extends Big
    final case class C1(n1: Int)   extends Big
    final case class C2(n2: Int)   extends Big
    final case class C3(n3: Int)   extends Big
    final case class C4(n4: Int)   extends Big
    final case class C5(n5: Int)   extends Big
    final case class C6(n6: Int)   extends Big
    final case class C7(n7: Int)   extends Big
    final case class C8(n8: Int)   extends Big
    final case class C9(n9: Int)   extends Big
    final case class C10(n10: Int) extends Big
    final case class C11(n11: Int) extends Big
    final case class C12(n12: Int) extends Big
    final case class C13(n13: Int) extends Big
    final case class C14(n14: Int) extends Big
    final case class C15(n15: Int) extends Big
    final case class C16(n16: Int) extends Big
    final case class C17(n17: Int) extends Big
    final case class C18(n18: Int) extends Big
    final case class C19(n19: Int) extends Big
    final case class C20(n20: Int) extends Big
    final case class C21(n21: Int) extends Big
    final case class C22(n22: Int) extends Big
    final case class C23(n23: Int) extends Big
    final case class C24(n24: Int) extends Big
    final case class C25(n25: Int) extends Big
    final case class C26(n26: Int) extends Big
    final case class C27(n27: Int) extends Big
    final case class C28(n28: Int) extends Big
    final case class C29(n29: Int) extends Big
    final case class C30(n30: Int) extends Big
    final case class C31(n31: Int) extends Big
    final case class C32(n32: Int) extends Big
    final case class C33(n33: Int) extends Big
    final case class C34(n34: Int) extends Big
    final case class C35(n35: Int) extends Big
    final case class C36(n36: Int) extends Big
    final case class C37(n37: Int) extends Big
    final case class C38(n38: Int) extends Big
    final case class C39(n39: Int) extends Big
    final case class C40(n40: Int) extends Big
    final case class C41(n41: Int) extends Big
    final case class C42(n42: Int) extends Big
    final case class C43(n43: Int) extends Big
    final case class C44(n44: Int) extends Big
    final case class C45(n45: Int) extends Big
    final case class C46(n46: Int) extends Big
    final case class C47(n47: Int) extends Big
    final case class C48(n48: Int) extends Big
    final case class C49(n49: Int) extends Big
    final case class C50(n50: Int) extends Big
    final case class C51(n51: Int) extends Big
    final case class C52(n52: Int) extends Big
    final case class C53(n53: Int) extends Big
    final case class C54(n54: Int) extends Big
    final case class C55(n55: Int) extends Big
    final case class C56(n56: Int) extends Big
    final case class C57(n57: Int) extends Big
    final case class C58(n58: Int) extends Big
    final case class C59(n59: Int) extends Big

  enum Wide derives WireCodec:
    case E0, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12, E13, E14, E15, E16, E17, E18, E19, E20, E21, E22, E23, E24, E25, E26, E27, E28, E29, E30, E31, E32, E33,
      E34, E35, E36, E37, E38, E39, E40, E41, E42, E43, E44, E45, E46, E47, E48, E49, E50, E51, E52, E53, E54, E55, E56, E57, E58, E59

  final case class Holder(big: Big, wide: Wide) derives WireCodec

  final case class Broad(
      f0: Option[String],
      f1: Option[String],
      f2: Option[String],
      f3: Option[String],
      f4: Option[String],
      f5: Option[String],
      f6: Option[String],
      f7: Option[String],
      f8: Option[String],
      f9: Option[String],
      f10: Option[String],
      f11: Option[String],
      f12: Option[String],
      f13: Option[String],
      f14: Option[String],
      f15: Option[String],
      f16: Option[String],
      f17: Option[String],
      f18: Option[String],
      f19: Option[String],
      f20: Option[String],
      f21: Option[String],
      f22: Option[String],
      f23: Option[String],
      f24: Option[String],
      f25: Option[String],
      f26: Option[String],
      f27: Option[String],
      f28: Option[String],
      f29: Option[String],
      f30: Option[String],
      f31: Option[String],
      f32: Option[String],
      f33: Option[String],
      f34: Option[String],
      f35: Option[String],
      f36: Option[String],
      f37: Option[String],
      f38: Option[String],
      f39: Option[String],
      f40: Option[String],
      f41: Option[String],
      f42: Option[String],
      f43: Option[String],
      f44: Option[String],
      f45: Option[String],
      f46: Option[String],
      f47: Option[String],
      f48: Option[String],
      f49: Option[String],
      f50: Option[String],
      f51: Option[String],
      f52: Option[String],
      f53: Option[String],
      f54: Option[String],
      f55: Option[String],
      f56: Option[String],
      f57: Option[String],
      f58: Option[String],
      f59: Option[String]
  ) derives WireCodec

final class WideDerivationSpec extends AnyWordSpec, Matchers:
  import WideDerivationSpec.*

  private def roundTrip[A](value: A)(using codec: WireCodec[A]): A =
    val document = BsonDocument()
    codec.encode(BsonDocumentWriter(document), value)
    codec.decode(BsonDocumentReader(document))

  "derivation" should {

    "handle a sum of sixty cases and an enum of sixty without raising -Xmax-inlines" in {
      roundTrip(Holder(Big.C59(59), Wide.E42)) shouldBe Holder(Big.C59(59), Wide.E42)
      roundTrip(Holder(Big.C0(0), Wide.E0)) shouldBe Holder(Big.C0(0), Wide.E0)
    }

    "handle a product of sixty fields" in {
      val broad = Broad(
        Some("x"),
        None,
        None,
        None,
        None,
        None,
        None,
        Some("x"),
        None,
        None,
        None,
        None,
        None,
        None,
        Some("x"),
        None,
        None,
        None,
        None,
        None,
        None,
        Some("x"),
        None,
        None,
        None,
        None,
        None,
        None,
        Some("x"),
        None,
        None,
        None,
        None,
        None,
        None,
        Some("x"),
        None,
        None,
        None,
        None,
        None,
        None,
        Some("x"),
        None,
        None,
        None,
        None,
        None,
        None,
        Some("x"),
        None,
        None,
        None,
        None,
        None,
        None,
        Some("x"),
        None,
        None,
        None
      )

      roundTrip(broad) shouldBe broad
    }
  }
