package mongo4s.cats

type CatsStream[F[*]] = [A] =>> fs2.Stream[F, A]
