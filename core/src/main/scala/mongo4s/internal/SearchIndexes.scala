package mongo4s.internal

import org.bson.BsonDocument
import com.mongodb.client.model.{SearchIndexModel, SearchIndexType}
import com.mongodb.reactivestreams.client.MongoCollection as RSMongoCollection

import mongo4s.{Effect, RsBridge}
import mongo4s.bson.FieldNaming
import mongo4s.operations.SearchIndex

import scala.jdk.CollectionConverters.given

private[mongo4s] object SearchIndexes:

  def create[F[*], S[*], A](collection: RSMongoCollection[?], indexes: Seq[SearchIndex[A]], naming: FieldNaming)(using
      F: Effect[F],
      rs: RsBridge[F, S],
  ): F[List[String]] =
    if indexes.isEmpty
    then F.pure(Nil)
    else F.suspend(rs.list(collection.createSearchIndexes(indexes.map(model(_, naming)).asJava)))

  def update[F[*], S[*], A](collection: RSMongoCollection[?], index: SearchIndex[A], naming: FieldNaming)(using
      F: Effect[F],
      rs: RsBridge[F, S],
  ): F[Unit] =
    F.suspend(rs.unit(collection.updateSearchIndex(index.name, index.definition(naming))))

  def drop[F[*], S[*]](collection: RSMongoCollection[?], name: String)(using rs: RsBridge[F, S]): F[Unit] =
    rs.unit(collection.dropSearchIndex(name))

  def list[F[*], S[*]](collection: RSMongoCollection[?])(using rs: RsBridge[F, S]): F[List[BsonDocument]] =
    rs.list(collection.listSearchIndexes(classOf[BsonDocument]))

  private def model[A](index: SearchIndex[A], naming: FieldNaming): SearchIndexModel =
    index match
      case SearchIndex.Search(name, _)    => SearchIndexModel(name, index.definition(naming), SearchIndexType.search())
      case SearchIndex.Vector(name, _, _) => SearchIndexModel(name, index.definition(naming), SearchIndexType.vectorSearch())

end SearchIndexes
