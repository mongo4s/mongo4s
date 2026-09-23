package mongo4s.repositories

final case class KeysetPage[E, K](items: List[E], next: Option[K])
