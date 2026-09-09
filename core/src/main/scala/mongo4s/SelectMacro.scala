package mongo4s

import scala.quoted.*
import scala.NamedTuple.AnyNamedTuple
import scala.annotation.publicInBinary

import mongo4s.operations.Projection
import mongo4s.queries.{FindQuery, SelectQuery}
import mongo4s.bson.{BsonDecoder, BsonDocumentDecoder, FieldNaming}

@publicInBinary private[mongo4s] object SelectMacro:

  def impl[F[*]: Type, S[*]: Type, A: Type, K <: AnyNamedTuple: Type](
      query: Expr[FindQuery[F, S, A]],
  )(using Quotes): Expr[SelectQuery[F, S, K]] =
    import quotes.reflect.*

    def fail(message: String): Nothing = report.errorAndAbort(message)

    def elementsOf(tuple: TypeRepr): List[TypeRepr] =
      tuple.dealias match
        case AppliedType(constructor, args) if constructor.typeSymbol.name.startsWith("Tuple") && args.sizeIs > 1 => args
        case AppliedType(constructor, List(head, tail)) if constructor.typeSymbol.name == "*:"                    => head :: elementsOf(tail)
        case AppliedType(constructor, List(single)) if constructor.typeSymbol.name == "Tuple1"                    => List(single)
        case other if other =:= TypeRepr.of[EmptyTuple]                                                           => Nil
        case other                                                                                                => fail(s"expected a tuple, got ${other.show}")

    val (labelTypes, valueTypes) = TypeRepr.of[K].dealias match
      case AppliedType(_, List(names, values)) => (elementsOf(names), elementsOf(values))
      case other                               => fail(s"selectAs needs a named tuple such as (name: String, age: Int), got ${other.show}")

    val labels = labelTypes.map {
      case ConstantType(StringConstant(name)) => name
      case other                              => fail(s"expected a field label, got ${other.show}")
    }

    if labels.isEmpty
    then fail("selectAs needs at least one field")

    val entity = TypeRepr.of[A]
    val fields = entity.typeSymbol.caseFields

    labels.zip(valueTypes).foreach { (label, requested) =>
      fields.find(_.name == label) match
        case None         =>
          val known = if fields.isEmpty then s"${entity.show} is not a case class" else s"its fields are ${fields.map(_.name).mkString(", ")}"
          fail(s"'$label' is not a field of ${entity.typeSymbol.name} ($known)")
        case Some(member) =>
          val declared = entity.memberType(member)
          if !(declared =:= requested)
          then fail(s"'$label' is ${declared.show} on ${entity.typeSymbol.name}, but selectAs asks for ${requested.show}")
    }

    val paths = Expr.ofList(labels.map(label => '{ FieldPath.derived(List(${ Expr(label) })) }))

    val decoder = '{ (naming: FieldNaming) =>
      BsonDocumentDecoder.instance[K] { document =>
        SelectSupport
          .sequence(${
            Expr.ofList(valueTypes.zipWithIndex.map { (tpe, index) =>
              tpe.asType match
                case '[t] =>
                  val instance = Expr
                    .summon[BsonDecoder[t]]
                    .getOrElse(fail(s"no BsonDecoder in scope for '${labels(index)}' of type ${tpe.show}"))

                  '{ SelectSupport.decodeField(document, ${ paths }.apply(${ Expr(index) }).render(naming), ${ instance }) }
            })
          })
          .map(values => Tuple.fromArray(values).asInstanceOf[K])
      }
    }

    '{ ${ query }.selecting[K](Projection.Include[A](${ paths }, withId = false), ${ decoder }) }
  end impl
