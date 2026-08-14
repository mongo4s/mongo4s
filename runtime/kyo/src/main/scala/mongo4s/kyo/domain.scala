package mongo4s.kyo

import kyo.{Abort, Async, Stream, <}

type KIO[A]     = A < (Async & Abort[Throwable])
type KStream[A] = Stream[A, Async & Abort[Throwable]]
