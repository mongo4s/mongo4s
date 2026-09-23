package mongo4s.testkit

import mongo4s.operations.Projection
import mongo4s.bson.BsonDocumentCodec
import mongo4s.{Effect, PrimaryKey, RsBridge}
import mongo4s.repositories.BaseMongoRepository

final class FakeRepository[F[*], S[*], E, K](
    val fake: FakeMongoCollection[F, S, E],
    override protected val batchSize: Int = BaseMongoRepository.DefaultBatchSize,
    projection: Projection[E] = Projection.empty[E],
)(using Effect[F], PrimaryKey[E, K])
    extends BaseMongoRepository[F, S, E, K](fake, batchSize, projection)

object FakeRepository:

  def apply[F[*], S[*], E, K](
      batchSize: Int = BaseMongoRepository.DefaultBatchSize,
      projection: Projection[E] = Projection.empty[E],
  )(using F: Effect[F], rs: RsBridge[F, S], codec: BsonDocumentCodec[E], pk: PrimaryKey[E, K]): FakeRepository[F, S, E, K] =
    new FakeRepository(
      fake = FakeMongoCollection[F, S, E](codec),
      batchSize = batchSize,
      projection = projection,
    )
