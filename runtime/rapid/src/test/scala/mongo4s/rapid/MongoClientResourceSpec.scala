package mongo4s.rapid

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import com.mongodb.reactivestreams.client.MongoClient as RSMongoClient

import mongo4s.rapid.RapidInstances.given

final class MongoClientResourceSpec extends AnyWordSpec, Matchers:

  private def recordingClient(closed: AtomicInteger): RSMongoClient =
    Proxy
      .newProxyInstance(
        classOf[RSMongoClient].getClassLoader,
        Array(classOf[RSMongoClient]),
        new InvocationHandler:
          def invoke(proxy: Object, method: Method, args: Array[Object]): Object =
            if method.getName == "close" then closed.incrementAndGet()
            null,
      )
      .asInstanceOf[RSMongoClient]

  "MongoClientResource.fromClient" should {
    "close the client when use returns normally" in {
      val closed = AtomicInteger(0)

      MongoClientResource.fromClient(recordingClient(closed))(_ => rapid.Task.pure(1)).sync() shouldBe 1

      closed.get shouldBe 1
    }

    "close the client when use throws while its task is being built" in {
      val closed = AtomicInteger(0)
      val boom   = RuntimeException("boom")

      val task = MongoClientResource.fromClient(recordingClient(closed))(_ => throw boom)

      a[RuntimeException] should be thrownBy task.sync()

      closed.get shouldBe 1
    }
  }
