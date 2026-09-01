package mongo4s.testkit

import mongo4s.bson.BsonDocumentCodec
import mongo4s.operations.Projection
import mongo4s.{Effect, PrimaryKey}
import mongo4s.repositories.BaseMongoRepository

final class FakeRepository[F[*], S[*], E, K](
    val fake: FakeMongoCollection[F, S, E],
    batchSize: Int = BaseMongoRepository.DefaultBatchSize,
    projection: Projection[E] = Projection.empty[E],
)(using Effect[F], PrimaryKey[E, K])
    extends BaseMongoRepository[F, S, E, K](fake, batchSize, projection)

object FakeRepository:

  def apply[F[*], S[*], E, K](
      emit: List[E] => S[E],
      batchSize: Int = BaseMongoRepository.DefaultBatchSize,
      projection: Projection[E] = Projection.empty[E],
  )(using F: Effect[F], codec: BsonDocumentCodec[E], pk: PrimaryKey[E, K]): FakeRepository[F, S, E, K] =
    new FakeRepository(FakeMongoCollection[F, S, E](codec, emit), batchSize, projection)
