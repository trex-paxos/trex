package com.github.trex_paxos.internals

import scala.concurrent._

trait SingletonActor[Rq,Rs] {
  private[this] implicit val context = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(1))
  protected def receive(rq: Rq): Rs
  def ask(m: Rq) = Future { receive(m) }
}