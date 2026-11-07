package mongo4s.it

import java.io.IOException
import java.net.{InetAddress, ServerSocket, Socket}
import java.util.concurrent.locks.LockSupport
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.cats.MongoClientResource

import scala.concurrent.duration.given

object LargeReplyItSpec:

  final class TricklingProxy(host: String, port: Int, chunkSize: Int, pauseNanos: Long) extends AutoCloseable:
    private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress)

    val chunks: AtomicInteger = AtomicInteger(0)

    def localPort: Int = server.getLocalPort

    private val acceptor = Thread.ofPlatform().daemon().start { () =>
      while !server.isClosed do
        try
          val client   = server.accept()
          val upstream = Socket(host, port)
          client.setTcpNoDelay(true)
          upstream.setTcpNoDelay(true)
          Thread.ofPlatform().daemon().start(() => pump(client, upstream, 64 * 1024, 0L, None)): Unit
          Thread.ofPlatform().daemon().start(() => pump(upstream, client, chunkSize, pauseNanos, Some(chunks))): Unit
        catch case _: IOException => ()
    }

    private def pump(from: Socket, to: Socket, size: Int, pause: Long, counted: Option[AtomicInteger]): Unit =
      val buffer = new Array[Byte](size)
      val in     = from.getInputStream
      val out    = to.getOutputStream
      try
        var read = in.read(buffer)
        while read >= 0 do
          out.write(buffer, 0, read)
          out.flush()
          counted.foreach(_.incrementAndGet(): Unit)
          if pause > 0 then LockSupport.parkNanos(pause)
          read = in.read(buffer)
      catch case _: IOException => ()
      finally
        from.close()
        to.close()
    end pump

    def close(): Unit =
      server.close()
      acceptor.join(1000)

final class LargeReplyItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import LargeReplyItSpec.TricklingProxy

  private val container = new MongoDBContainer("mongo:8.2")

  private lazy val proxy = TricklingProxy(container.getHost, container.getMappedPort(27017), 1024, 50_000L)

  override def beforeAll(): Unit = container.start()

  override def afterAll(): Unit =
    proxy.close()
    container.stop()

  private val payload = "x" * 30_000

  private val documents = (1 to 100).toList.map(i => BsonDocument().append("i", BsonInt32(i)).append("payload", BsonString(payload)))

  "a find whose reply reaches the client a kilobyte at a time" should {

    "complete rather than overflow the driver's stack in the callback of every partial read" in {
      val program =
        MongoClientResource.fromConnectionString[IO](s"mongodb://127.0.0.1:${proxy.localPort}/?directConnection=true").use { client =>
          for
            database   <- client.getDatabase("test")
            collection <- database.getCollection[BsonDocument]("blobs")
            _          <- collection.insertMany(documents)
            before     <- IO(proxy.chunks.get)
            found      <- collection.find().all
            after      <- IO(proxy.chunks.get)
          yield (found.size, after - before)
        }

      program.timeout(60.seconds).asserting { (found, chunks) =>
        found shouldBe 100
        chunks should be > 2_500
      }
    }
  }
