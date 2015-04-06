package org.cscie54

import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor._
import akka.actor.{PoisonPill, Actor, Props, ActorSystem}
import akka.pattern.ask
import akka.routing._

import akka.util.Timeout


import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import scala.concurrent.{Promise, Future}
import scala.concurrent.duration._
import scala.collection.{mutable, Iterable}
import scala.util.{Failure, Success}

/**
 * Actor based implementation of a ConcurrentHashMap
 * @param concurrencyLevel number of threads that can concurrently perform operations on the ConcurrentHashMap
 * @param actorSystem actor system used for actors
 */
class ConcurrentHashMapImpl(concurrencyLevel:Int)(implicit actorSystem: ActorSystem) extends ConcurrentHashMap {

  val allMapActors = scala.collection.mutable.Map.empty[Integer, ActorRef]

  // not sure if I need wrap initialization in the default constructor...
  for (index <- 0 until concurrencyLevel) {
    allMapActors.put(index, actorSystem.actorOf(Props[ConcurrentHashMapActor]))
  }

  private def getActorIndex(key: K): Integer = {
    (key.hashCode() & 0x7fffffff) % concurrencyLevel
  }

  // create router actor

  implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  def get(key: K): Future[Option[V]] = {

    val actorIndex = getActorIndex(key)

    val actorToTalkTo = allMapActors(actorIndex)

    val future = actorToTalkTo.ask(Get(key))

    return future.mapTo[Option[V]]
  }

  def put(key: K, value: V): Future[Unit] = {

    val actorIndex = getActorIndex(key)

    val actorToTalkTo = allMapActors(actorIndex)

    Future {
      actorToTalkTo ! Put(key, value)
    }

  }

  def clear(): Future[Unit] = {
    Future {

      for (index <- 0 until concurrencyLevel) {
        val actorToTalkTo = allMapActors(index)

        val future = actorToTalkTo ! (Clear())
      }
    }

  }


  def toIterable: Future[Iterable[(K, V)]] = {
    val listOfFutureOptionOfMap: ListBuffer[Future[ListBuffer[(K, V)]]] = ListBuffer()

    // collect Futures from all Actors
    for (index <- 0 until concurrencyLevel) {
      val actorToTalkTo = allMapActors(index)

      val future = actorToTalkTo.ask(GetPartition())

      listOfFutureOptionOfMap.+=(future.mapTo[ListBuffer[(K, V)]])
    }

    // make it all one Future
    val futureOfListOfOptionOfMap = Future.sequence(listOfFutureOptionOfMap)

    //val listOfKvFuture: Future[ListBuffer[(K, V)]] = Future{ListBuffer()}

    for {
          listOfOptionMap <- futureOfListOfOptionOfMap
          //listOfKv <- listOfKvFuture
          listOfKv = listOfOptionMap.flatten

        } yield listOfKv.to[Iterable]

  }

/*
  def mapReduce(map: (K, V) => U, reduce: (U, U) => U): Future[U] =
  {

    val listOfFutureOptionOfMap: ListBuffer[Future[Option[scala.collection.mutable.Map[K, V]]]] = ListBuffer()

    // collect Futures from all Actors
    for (index <- 0 until concurrencyLevel) {
      val actorToTalkTo = allMapActors(index)

      val future = actorToTalkTo.ask(GetPartition())

      listOfFutureOptionOfMap.+=(future.mapTo[Option[scala.collection.mutable.Map[K, V]]])
    }

    // make it all one Future
    val futureOfListOfOptionOfMap = Future.sequence(listOfFutureOptionOfMap)

    val listOfKv: ListBuffer[(K, V)] = ListBuffer()
    val listOfUs: ListBuffer[U] = ListBuffer()

    for {
        //val listOfKv: ListBuffer[(K, V)] = ListBuffer()
        listOfOptionMap <- futureOfListOfOptionOfMap

        temp = listOfOptionMap.map {

          optionOfMap => optionOfMap match
          {
              case Some(myMap) => val myList = myMap.toList
                listOfKv ++ myList.to[ListBuffer]
              case None => "?" //do nothing, ignore!
            }
        }

        tempKV = listOfKv.map {

          kv => listOfUs.+(map(kv._1,kv._2))

        }

        resultU = helperReduce ( listOfUs, reduce: (U, U) => U )

    } yield resultU
  }

*/

 /*
  private def helperReduce ( listOfUs: ListBuffer[U], reduce: (U, U) => U ) : U = {

    if (1 == listOfUs.length) {
      return listOfUs(0)
    }
    else
    {
      @tailrec
      def reduceAccumulator(listOfUs: List[U], accum: U): U = {
        listOfUs match {
          case Nil => accum
          case u :: tail => reduceAccumulator(tail, reduce(accum,u))
        }
      }
      reduceAccumulator(listOfUs.toList, listOfUs(0))

    }
  }
*/

  //def failFastIterator: Future[Iterable[(K, V)]] = ???

}


class ConcurrentHashMapActor extends Actor
{
  val myMap = scala.collection.mutable.Map.empty[K, V] //scala.collection.immutable.Map[K,V]// or use var with immutable map

  def receive = {

    case Put(key, value) => myMap(key) = value

    case Get(key) =>  if(myMap.contains(key))
                      {
                        sender() ! myMap.get(key)
                      }
                      else
                      {
                        sender() ! None
                      }

    case Clear() => myMap.clear()

    case GetPartition() => sender() ! myMap.to[ListBuffer] //don't care if map is empty
  }


}

case class Get(key: K)

case class Clear()

case class Put(key: K, value: V)

case class GetPartition()
