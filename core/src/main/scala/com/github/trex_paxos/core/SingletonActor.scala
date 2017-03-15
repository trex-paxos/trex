package com.github.trex_paxos.core

import scala.concurrent._

object SingletonActor {
  implicit val context = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(1))
}

trait SingletonActor[Rq,Rs] {
  protected def receive(rq: Rq): Rs
  private[this] implicit val ctx = SingletonActor.context
  def ?(m: Rq) = Future { receive(m) }
}