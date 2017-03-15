package com.github.trex_paxos.core

//http://stackoverflow.com/a/16363444/329496
object DelayedFuture {

  import java.util.{Timer, TimerTask}
  import scala.concurrent._
  import scala.concurrent.duration.FiniteDuration
  import scala.util.Try

  // a single timer demon thread which schedules the passed work to run on the implicit execution context
  private val timer = new Timer(true)

  private def makeTask[T](body: => T)(schedule: TimerTask => Unit)(implicit ctx: ExecutionContext): Future[T] = {
    val prom = Promise[T]()
    schedule(
      new TimerTask {
        def run() {
          ctx.execute(
            new Runnable {
              def run() {
                prom.complete(Try(body))
              }
            }
          )
        }
      }
    )
    prom.future
  }

  private def runTask[T](body: => T)(schedule: TimerTask => Unit)(implicit ctx: ExecutionContext): Unit = {
    schedule(
      new TimerTask {
        def run() {
          ctx.execute(
            new Runnable {
              def run() {
                body
              }
            }
          )
        }
      }
    )
  }

  /**
    * Schedule an action at a point in the future such as a timeout operation.
    *
    * @param duration The delay.
    * @param body     The work to happen.
    */
  def apply[T](duration: FiniteDuration)(body: => T)(implicit ctx: ExecutionContext): Future[T] = {
    makeTask(body)(timer.schedule(_, duration.toMillis))
  }

  /**
    * Schedule an action to repeat periodically after a delay such as emitting a heartbeat.
    *
    * @param delay  The delay before starting in ms.
    * @param period The repeat interal in ms.
    * @param body   The work to happen.
    */
  def apply[T](delay: Long, period: Long)(body: => T)(implicit ctx: ExecutionContext): Unit = {
    runTask(body)(timer.schedule(_, delay, period))
  }
}