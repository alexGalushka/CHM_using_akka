package org.cscie54

import scala.concurrent.Future

trait ConcurrentHashMap {
  def get(key: K): Future[Option[V]]
  def put(key: K, value: V): Future[Unit]
  def toIterable: Future[Iterable[(K, V)]]
  def mapReduce(map: (K, V) => U, reduce: (U, U) => U): Future[U]
  def clear(): Future[Unit]
  def failFastToIterable: Future[Iterable[(K, V)]]
}

