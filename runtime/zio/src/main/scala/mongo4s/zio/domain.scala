package mongo4s.zio

import zio.stream.ZStream

type ZioStream = [A] =>> ZStream[Any, Throwable, A]
