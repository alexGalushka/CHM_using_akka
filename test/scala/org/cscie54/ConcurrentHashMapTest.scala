package org.cscie54.a2

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.dispatch.Futures
import akka.testkit.TestKit
import akka.util.Timeout
import org.cscie54.{U, V, K, ConcurrentHashMapImpl}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
//import scala.async.Async.{async, await}

class ConcurrentHashMapTest extends TestKit(ActorSystem("ConcurrentHashMapTest"))
  with FlatSpecLike with Matchers with ScalaFutures with BeforeAndAfterAll with ParallelTestExecution {


  def myMap(key: K, value: V): U =
  {
    key
  }

  def myReduce(key1:U, key2:U): U =
  {
    List(key1, key2).reduce(_ + _)
  }


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


  "A concurrent hash map" should "be iterable" in {
    val map = new ConcurrentHashMapImpl(16)

    val key1 = "hello, world"
    val value1 = 0

    val key2 = "hello"
    val value2 = 1

    val key3 = "world"
    val value3 = 2

    val key4 = "future"
    val value4 = 3

    val key5 = "past"
    val value5 = 3

    val key6 = "promise"
    val value6 = 5

    val listOfKeys = List(key1, key2, key3, key4, key5, key6)
    val listOfValues = List(value1, value2, value3, value4, value5, value6)


    val f1 = map.put(key1, value1)
    val f2 = map.put(key2, value2)
    val f3 = map.put(key3, value3)
    val f4 = map.put(key4, value4)
    val f5 = map.put(key5, value5)
    val f6 = map.put(key6, value6)


    val f = Future.sequence(List(f1,f2,f3, f4, f5, f6))

    whenReady(f)
    { _ =>
      whenReady(map.toIterable) {
        for ( itera <- _ )
        {
             listOfKeys  should contain (itera._1)
             listOfValues should contain (itera._2)
        }
      }
    }

  }


/*
  "A concurrent hash map" should "be wrapped by mapReduce" in {
    val map = new ConcurrentHashMapImpl(16)

    val key1 = "hi+"
    val value1 = 0

    val key2 = "hello+"
    val value2 = 1

    val key3 = "world+"
    val value3 = 2

    val key4 = "future+"
    val value4 = 3

    val key5 = "past+"
    val value5 = 3

    val key6 = "promise+"
    val value6 = 5


    val resultString = "hi+hello+world+future+past+promise+"

    val f1 = map.put(key1, value1)
    val f2 = map.put(key2, value2)
    val f3 = map.put(key3, value3)
    val f4 = map.put(key4, value4)
    val f5 = map.put(key5, value5)
    val f6 = map.put(key6, value6)


    val f = Future.sequence(List(f1,f2,f3, f4, f5, f6))

    f onComplete {
      case Success(good) =>
        whenReady(map.mapReduce(myMap: (K, V) => U, myReduce: (U, U) => U )) {
          _ should be(resultString)
        }
      case Failure(t) => println("An error has occured: " + t.getMessage)
    }

    /*
    whenReady(f)
    { _ =>
      whenReady(map.mapReduce(myMap: (K, V) => U, myReduce: (U, U) => U )) {
        _ should be(resultString)
      }
    }

    */

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