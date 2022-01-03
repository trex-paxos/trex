package com.github.trex_paxos.library

case class JournalBounds(min: Long, max: Long)

object Journal {
  // timed-out nodes issue Prepare(BallotNumber(0, 0)) and so we start at one hiher
  val minNumber = BallotNumber(1, 1)
  val minBookwork = Progress(minNumber, Identifier(0, minNumber,0))
}

/** 
 *  Durable store for the replicated log. Methods load and save must make durable the bookwork of the progress made by the given cluster node.
 *  The accept and accepted methods store value message at a given log index.
 *  An implementation could use a BTree else a local database.
 */
trait Journal {

  /**
   * Synchronously journal progress bookwork. 
   * @param progress The highest committed and highest promised of this cluster node. 
   */
  def saveProgress(progress: Progress): Unit
  
  /**
   * Load progress bookwork. 
   * @return The maximum promise and the highest commit identifier. 
   */
  def loadProgress(): Progress

  /**
   * Synchronously save an accept message in its slot specified by the logIndex in the [[Identifier]] of the message.
   * Callers *must* ensure that values with a low [[BallotNumber]] do not overwrite values with a higher number.
   */
  def accept(a: Accept*): Unit
  
  /**
   * Load the accept message from the specified slot. 
   * @return The accept message at the requested slot if any. 
   */
  def accepted(logIndex: Long): Option[Accept]
  
  /**
   * The lowest and highest index currently available. 
   * @return The min and max index retrained within this journal which is subject to the retention policy of the implementation. 
   */
  def bounds(): JournalBounds

}
