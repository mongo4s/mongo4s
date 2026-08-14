package mongo4s

trait Streamable[S[*], A]

object Streamable:
  def instance[S[*], A]: Streamable[S, A] = new Streamable[S, A] {}
