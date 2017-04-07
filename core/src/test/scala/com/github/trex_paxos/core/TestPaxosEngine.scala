package com.github.trex_paxos.core

import com.github.trex_paxos.PaxosProperties
import com.github.trex_paxos.library._

import scala.util.Try

object TestPaxosEngine {
  val deliverMembership: PartialFunction[Payload, Array[Byte]] = ???
  val deliverClient: PartialFunction[Payload, Array[Byte]] = ???
  val serialize: (Any) => Try[Array[Byte]] = ???
  val transmitMessages: (Seq[PaxosMessage]) => Unit = ???
}

class TestPaxosEngine(override val paxosProperties: PaxosProperties, override val journal: Journal, override val initialAgent: PaxosAgent)
  extends PaxosEngine(paxosProperties, journal, initialAgent, TestPaxosEngine.deliverMembership, TestPaxosEngine.deliverClient, TestPaxosEngine.serialize, TestPaxosEngine.transmitMessages) {
  override def logger: PaxosLogging = NoopPaxosLogging

  override def deliver(payload: Payload): Array[Byte] = ???

  override def associate(value: CommandValue, id: Identifier): Unit = ???

  override def respond(results: Option[Map[Identifier, Any]]): Unit = ???

}
