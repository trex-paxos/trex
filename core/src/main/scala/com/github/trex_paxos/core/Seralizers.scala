package com.github.trex_paxos.core

import scala.util.Try

/**
  * This is a do nothing Serializer which assumes that the host application has already turned the payload into a byte array.
  */
object ByteArraySerializer {
  def serialize(o: Any): Try[Array[Byte]] = {
    Try{
      o.asInstanceOf[Array[Byte]]
    }
  }
  def deserialize(bs: Array[Byte]): Try[Any] = Try{bs}
}
