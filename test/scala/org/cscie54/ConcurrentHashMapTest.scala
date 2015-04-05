package org.cscie54.a2

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.dispatch.Futures
import akka.testkit.TestKit
import akka.util.Timeout
import org.cscie54.ConcurrentHashMapImpl
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class ConcurrentHashMapTest extends TestKit(ActorSystem("ConcurrentHashMapTest"))
  with FlatSpecLike with Matchers with ScalaFutures with BeforeAndAfterAll with ParallelTestExecution {

  //implicit val timeout = Timeout(20, TimeUnit.SECONDS)

  "A concurrent hash map" should "put" in {
    val map = new ConcurrentHashMapImpl(16)
    val key = "hello, world"
    val value = 0

    whenReady(map.put(key, value)) { _ =>
      whenReady(map.get(key)) {
        _ should be(Option(value))
      }
    }

    // val pony = map.put(key, value)
    //val res = Await.result(pony, Duration("10 seconds"))
    //map.get(key) should be (Option(value))

  }

  it should "clear" in {
    val map = new ConcurrentHashMapImpl(16)
    val key = "hello, world"
    val value = 0

    /*
    whenReady(map.put(key, value)){ _ =>
      whenReady(map.clear()) {
        map. should be (0)
      }
    }
*/
    // val listFutures: List[Future]

    //val future: Future[List] = Future.sequesnce(listFutures)


  }

  "A concurrent hash map" should "get null on nonexistent key" in {
    val map = new ConcurrentHashMapImpl(16)
    val key = "hello, world"
    val value = 0

    whenReady(map.put(key, value)) { _ =>
      whenReady(map.get("bye")) {
        _ should be(None)
      }
    }

  }
/*
  "A concurrent hash map" should "be iterable" in {
    val map = new ConcurrentHashMapImpl(16)
    val key1 = "hello, world"
    val value1 = 0

    val key2 = "hello, world"
    val value2 = 1

    whenReady(map.put(key, value)) { _ =>
      whenReady(map.get("bye")) {
        _ should be(None)
      }
    }

  }
*/


  "A concurrent hash map value" should "be overitten when the same key used" in {
    val map = new ConcurrentHashMapImpl(16)
    val key1 = "hello, world"
    val value1 = 0
    val value2 = 1

    whenReady(map.put(key1, value1)) { _ =>
      whenReady(map.put(key1, value2)) { _ =>
        whenReady(map.get(key1)) {
          _ should be(Option(value2))
        }
      }
    }

  }


  /*
  it should "clear" in {
    val map = new ConcurrentHashMapImpl(16)
    val key = "hello, world"
    val value = 0

    whenReady(put)
      whenReady(clear)
        map.size should be (0)


    val listFutures: List[Future]

    val future: Future[List] = Future.sequesnce(listFutures)


  }

  it should "clear" in {
    val map = new ConcurrentHashMapImpl(16)
    val key = "hello, world"
    val value = 0
    val key = "hi"
    val value = 42

    val f1 = map.put(key, value)
    val f2 = map.put(key2, value2)

    val f = Futures.sequence(List(f1,f2))

    whenReady(f)
      map.toIterable //make sure this is equal to the keys and values

    val listFutures: List[Future]

    val future: Future[List] = Future.sequesnce(listFutures)


  }


  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)
  */

}