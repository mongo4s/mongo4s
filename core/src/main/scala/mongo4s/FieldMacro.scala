package mongo4s

import scala.quoted.*
import scala.annotation.publicInBinary

import mongo4s.bson.direct.WireFields

@publicInBinary private[mongo4s] object FieldMacro:

  def impl[E: Type, A: Type](selector: Expr[E => A])(using Quotes): Expr[Field[E, A]] =
    import quotes.reflect.*

    def fail(message: String): Nothing = report.errorAndAbort(message)

    def verifyField(owner: TypeRepr, name: String, whole: Term): (TypeRepr, Expr[FieldPath.Segment]) =
      val symbol = owner.typeSymbol
      val field  = symbol.caseFields.find(_.name == name)
      val param  = WireFields.params(symbol).find(_.name == name)

      field match
        case Some(member) =>
          if param.exists(WireFields.isNotStored)
          then fail(s"'$name' of ${symbol.name} is @notStored, so no document holds it to query; got: ${whole.show}")

          val segment = param.flatMap(WireFields.storedName) match
            case Some(stored) => '{ FieldPath.Segment.Stored(${ Expr(stored) }) }
            case None         => '{ FieldPath.Segment.Derived(${ Expr(name) }) }

          (owner.memberType(member), segment)
        case None         =>
          val known =
            if symbol.caseFields.isEmpty then "it is not a case class"
            else s"its fields are ${symbol.caseFields.map(_.name).mkString(", ")}"

          fail(
            s"'$name' is not a field of ${symbol.name} ($known). " +
              s"Field.of only accepts case-class field selectors such as _.field or _.nested.field; " +
              s"got: ${whole.show}. " +
              s"To reference a stored name directly — including inside an array — use Field.stored."
          )
    end verifyField

    def segmentsOf(term: Term): List[String] =
      def loop(current: Term, acc: List[String]): List[String] = current match
        case Inlined(_, _, inner)               => loop(inner, acc)
        case Typed(inner, _)                    => loop(inner, acc)
        case Block(Nil, inner)                  => loop(inner, acc)
        case Block(List(definition: DefDef), _) => definition.rhs.fold(fail(s"Expected a field selector, got: ${term.show}"))(loop(_, acc))
        case Lambda(_, body)                    => loop(body, acc)
        case Select(qualifier, name)            => loop(qualifier, name :: acc)
        case Ident(_)                           => acc
        case other                              => fail(s"Expected a field selector such as _.field or _.nested.field, got: ${other.show}")

      loop(term, Nil)
    end segmentsOf

    val root     = TypeRepr.of[E]
    val segments = segmentsOf(selector.asTerm)

    if segments.isEmpty
    then fail("Field selector must reference at least one field")

    val (_, rendered) = segments.foldLeft((root, List.empty[Expr[FieldPath.Segment]])) { case ((owner, done), name) =>
      val (next, segment) = verifyField(owner.widen, name, selector.asTerm)
      (next, done :+ segment)
    }

    '{ Field[E, A](FieldPath(${ Expr.ofList(rendered) })) }
  end impl
