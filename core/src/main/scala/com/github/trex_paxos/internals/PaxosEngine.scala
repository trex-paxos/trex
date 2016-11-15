package com.github.trex_paxos.internals

import com.github.trex_paxos.library._

class PaxosEngine(io: PaxosIO, initialAgent: PaxosAgent) extends SingletonActor[PaxosEvent, Seq[PaxosMessage]] {

  private[this] var agent = initialAgent

  private val paxosAlgorithm = new PaxosAlgorithm

  class CollectionIo(io: PaxosIO) extends PaxosIO {
    var messages: Seq[PaxosMessage] = Seq()
    override def journal: Journal = io.journal
    override def logger: PaxosLogging = io.logger
    override def randomTimeout: Long = io.randomTimeout
    override def clock: Long = io.clock
    override def deliver(payload: Payload): Any = ???
    override def send(msg: PaxosMessage): Unit = {
      messages = messages :+ msg
    }
    override def senderId(): String = ???
    override def respond(client: String, data: Any): Unit = io.respond(client, data)
    override def sendNoLongerLeader(clientCommands: Map[Identifier, (CommandValue, String)]): Unit = io.sendNoLongerLeader(clientCommands)
}

  override protected def receive(e: PaxosEvent): Seq[PaxosMessage] = {
    val collectionIO = new CollectionIo(e.io)
    val ce = e.copy(io = collectionIO)
    agent = paxosAlgorithm(e)
    collectionIO.messages
  }
}
