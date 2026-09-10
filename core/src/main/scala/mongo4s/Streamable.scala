package mongo4s

@annotation.implicitNotFound(
  "No Streamable[${S}, ${A}]. A runtime module supplies one for its own stream type — import that module's " +
    "`given`s (mongo4s.cats.CatsInstances.given and so on). Some backends carry per-element evidence in it, so it " +
    "cannot be constructed from outside."
)
trait Streamable[S[*], A]

object Streamable:
  private[mongo4s] def instance[S[*], A]: Streamable[S, A] = new Streamable[S, A] {}
