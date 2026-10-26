package mongo4s.bson.direct

import scala.annotation.publicInBinary
import scala.quoted.{Expr, Quotes, Type, Varargs}

object WireDefaults:

  inline def of[A]: Map[Int, () => Any] = ${ defaultsOf[A] }

  @publicInBinary private[direct] def defaultsOf[A: Type](using quotes: Quotes): Expr[Map[Int, () => Any]] =
    import quotes.reflect.*

    val tpe       = TypeRepr.of[A].dealias
    val owner     = tpe.typeSymbol
    val companion = owner.companionModule

    val typeArgs = tpe match
      case AppliedType(_, args) => args
      case _                    => Nil

    val params =
      if !owner.isClassDef || companion == Symbol.noSymbol
      then Nil
      else owner.primaryConstructor.paramSymss.find(_.headOption.exists(_.isTerm)).getOrElse(Nil)

    val entries = params.zipWithIndex.collect {
      case (param, index) if param.flags.is(Flags.HasDefault) =>
        companion.declaredMethod(s"$$lessinit$$greater$$default$$${index + 1}").headOption.map { getter =>
          val selected = Ref(companion).select(getter)
          val applied  = if getter.paramSymss.headOption.exists(_.exists(_.isType)) then selected.appliedToTypes(typeArgs) else selected
          '{ ${ Expr(index) } -> (() => ${ applied.asExprOf[Any] }) }
        }
    }.flatten

    '{ Map[Int, () => Any](${ Varargs(entries) }*) }
  end defaultsOf
