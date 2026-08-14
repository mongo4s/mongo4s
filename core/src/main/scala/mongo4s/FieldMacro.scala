package mongo4s

import scala.quoted.*

private[mongo4s] object FieldMacro:

  def impl[E: Type, A: Type](selector: Expr[E => A])(using Quotes): Expr[Field[E, A]] =
    import quotes.reflect.*

    def fail(term: Term): Nothing =
      report.errorAndAbort(s"Expected a field selector such as _.field or _.nested.field, got: ${term.show}")

    def extract(term: Term, acc: List[String]): List[String] =
      term match
        case Inlined(_, _, inner)               => extract(inner, acc)
        case Typed(inner, _)                    => extract(inner, acc)
        case Block(Nil, inner)                  => extract(inner, acc)
        case Block(List(definition: DefDef), _) => definition.rhs.fold(fail(term))(extract(_, acc))
        case Lambda(_, body)                    => extract(body, acc)
        case Select(qualifier, name)            => extract(qualifier, name :: acc)
        case Ident(_)                           => acc
        case other                              => fail(other)

    val segments = extract(selector.asTerm, Nil)

    if segments.isEmpty then report.errorAndAbort("Field selector must reference at least one field")

    '{ Field[E, A](FieldPath(${ Expr(segments) })) }
