package mongo4s.internal

import org.bson.BsonDocument
import org.reactivestreams.Publisher
import com.mongodb.reactivestreams.client.FindPublisher

import mongo4s.{RsBridge, Streamable}
import mongo4s.queries.{DecodeAttempts, SelectQuery}
import mongo4s.bson.{BsonDocumentDecoder, DecodeResult}

private[mongo4s] object SelectQueryImpl:

  def apply[F[*], S[*], B](
      documents: Option[Int] => FindPublisher[BsonDocument],
      limit: Option[Int],
      decoder: BsonDocumentDecoder[B],
      live: Boolean,
  )(using rs: RsBridge[F, S]): SelectQuery[F, S, B] =
    val decode = decoder.decodeDocument

    def publisher(effectiveLimit: Option[Int]): Publisher[B] = DecodingPublisher(documents(effectiveLimit), decode)

    def attemptingPublisher(effectiveLimit: Option[Int]): Publisher[DecodeResult[B]] =
      AttemptingPublisher(documents(effectiveLimit), decode)

    def streamed[C](publisher: => Publisher[C])(using Streamable[S, C]): S[C] =
      if live
      then rs.liveStream(publisher)
      else rs.stream(publisher)

    new SelectQuery[F, S, B]:
      def first: F[Option[B]]                  = rs.option(publisher(Some(1)))
      def all: F[List[B]]                      = rs.list(publisher(limit))
      def stream(using Streamable[S, B]): S[B] = streamed(publisher(limit))

      def attempting: DecodeAttempts[F, S, B] = new DecodeAttempts[F, S, B]:
        def all: F[List[DecodeResult[B]]]                                    = rs.list(attemptingPublisher(limit))
        def stream(using Streamable[S, DecodeResult[B]]): S[DecodeResult[B]] = streamed(attemptingPublisher(limit))
  end apply
