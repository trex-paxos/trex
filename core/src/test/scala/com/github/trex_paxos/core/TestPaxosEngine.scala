package com.github.trex_paxos.core

import com.github.trex_paxos.PaxosProperties
import com.github.trex_paxos.library._

class TestPaxosEngine(val paxosProperties: PaxosProperties, val journal: Journal, val initialAgent: PaxosAgent)
  extends PaxosEngine {
  override def logger: PaxosLogging = NoopPaxosLogging

  override def deliver(payload: Payload): Any = ???

  override def associate(value: CommandValue, id: Identifier): Unit = ???

  override def respond(results: Option[Map[Identifier, Any]]): Unit = ???

}
