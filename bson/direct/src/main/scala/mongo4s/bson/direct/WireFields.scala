package mongo4s.bson.direct

import scala.annotation.publicInBinary
import scala.quoted.{Expr, Quotes, Type, Varargs}

import org.bson.{BsonReader, BsonWriter}

object WireFields:

  inline def storedNames[A]: Array[String] = ${ storedNamesOf[A] }

  inline def notStored[A]: Array[Boolean] = ${ notStoredOf[A] }

  inline def codecs[A]: () => Array[WireCodec[Any]] = ${ codecsOf[A] }

  @publicInBinary private[mongo4s] def params(using quotes: Quotes)(owner: quotes.reflect.Symbol): List[quotes.reflect.Symbol] =
    owner.primaryConstructor.paramSymss.find(_.headOption.exists(_.isTerm)).getOrElse(Nil)

  @publicInBinary private[mongo4s] def storedName(using quotes: Quotes)(param: quotes.reflect.Symbol): Option[String] =
    import quotes.reflect.*

    param.getAnnotation(TypeRepr.of[stored].typeSymbol).map {
      case Apply(_, List(Literal(StringConstant(name)))) if name.nonEmpty => name
      case other                                                          =>
        report.errorAndAbort(s"@stored on '${param.name}' takes the stored name as a non-empty string literal, got ${other.show}")
    }
  end storedName

  @publicInBinary private[mongo4s] def isNotStored(using quotes: Quotes)(param: quotes.reflect.Symbol): Boolean =
    import quotes.reflect.*

    param.hasAnnotation(TypeRepr.of[notStored].typeSymbol)

  @publicInBinary private[direct] def storedNamesOf[A: Type](using quotes: Quotes): Expr[Array[String]] =
    import quotes.reflect.*

    val names = params(TypeRepr.of[A].typeSymbol).map(param => storedName(param).fold('{ null: String })(name => Expr(name)))
    '{ Array[String](${ Varargs(names) }*) }

  @publicInBinary private[direct] def notStoredOf[A: Type](using quotes: Quotes): Expr[Array[Boolean]] =
    import quotes.reflect.*

    val flags = params(TypeRepr.of[A].typeSymbol).map(param => Expr(isNotStored(param)))
    '{ Array[Boolean](${ Varargs(flags) }*) }

  @publicInBinary private[direct] def codecsOf[A: Type](using quotes: Quotes): Expr[() => Array[WireCodec[Any]]] =
    import quotes.reflect.*

    val tpe   = TypeRepr.of[A]
    val owner = tpe.typeSymbol

    val codecs = params(owner).map { param =>
      tpe.memberType(owner.caseFields.find(_.name == param.name).getOrElse(param)).asType match
        case '[t] =>
          if isNotStored(param)
          then
            if param.flags.is(Flags.HasDefault) then '{ NotStoredCodec }
            else if TypeRepr.of[t] <:< TypeRepr.of[Option[?]] then '{ NotStoredOptionCodec }
            else report.errorAndAbort(s"@notStored '${param.name}' of ${owner.name} is never read, so it needs a default value or an Option type")
          else
            Expr.summon[WireCodec[t]] match
              case Some(codec) => '{ $codec.asInstanceOf[WireCodec[Any]] }
              case None        =>
                report.errorAndAbort(
                  s"No WireCodec[${Type.show[t]}] for the field '${param.name}' of ${owner.name}. Give the type `derives WireCodec`, " +
                    "write a given for it, or mark the field @notStored if it is not to be kept in the document."
                )
    }

    '{ () => Array[WireCodec[Any]](${ Varargs(codecs) }*) }
  end codecsOf

  @publicInBinary private[direct] object NotStoredCodec extends WireCodec[Any]:
    def encode(writer: BsonWriter, value: Any): Unit = throw IllegalStateException("a @notStored field is never written")
    def decode(reader: BsonReader): Any              = throw IllegalStateException("a @notStored field is never read")

  @publicInBinary private[direct] object NotStoredOptionCodec extends WireCodec[Any]:
    def encode(writer: BsonWriter, value: Any): Unit = throw IllegalStateException("a @notStored field is never written")
    def decode(reader: BsonReader): Any              = throw IllegalStateException("a @notStored field is never read")
    override def defaultOnMissing: Option[Any]       = Some(None)
