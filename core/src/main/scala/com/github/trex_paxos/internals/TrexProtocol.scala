package com.github.trex_paxos.internals

import com.github.trex_paxos.library.CommandValue

/**
  * Client request command has an id to correlate to the server response.
  */
private[trex_paxos] case class ClientRequestCommandValue(msgId: Long, val bytes: Array[Byte]) extends CommandValue

/**
  * Placeholder currently not implemented.
  *
  * There are a number of optimisations that are possible with read-only work. These trade consistency for speed and
  * scalability; possibly even offloading the read to a follower. These case classes let you wrap your work in case
  * classes that tag the work as optimisable. Whether the work is optimised, and how, will depend on the current feature
  * set and current configuration of trex.
  */
sealed trait OptimizableReadOnlyWork {
  def raw: AnyRef
}

/**
  * Placeholder currently not implemented.
  *
  * A `Strong` read is a read of committed work which is always strongly ordered with respect to writes. This requires
  * the leader to assign an order to reads and writes upon arrive, then only run the reads when it learns that all
  * preceeding writes have been chosen. It then must return the results in the same order.
  *
  * @param raw The actual raw client command which is opaque to Trex.
  */
case class StrongReadWork(val raw: AnyRef) extends OptimizableReadOnlyWork

/**
  * Placeholder currently not implemented.
  *
  * A `Timeline` read is a read of committed work where a single thread (a single timeline) will see it's own writes but
  * its reads may see stale (cached) reads with respect to write made on separate timelines (other client processes or
  * other threads in the same process).
  *
  * This case class marks client work as safe to stale cached reads such that it is eligible to be optimised as a single
  * read if the cluster is configured to allow the optimisation.
  *
  * @param raw The actual raw client command which is opaque to Trex.
  */
case class TimelineReadWork(val raw: AnyRef) extends OptimizableReadOnlyWork

/**
  * Placeholder currently not implemented.
  *
  * An `Outdated` read is read work which is safe to read directly from a replica. No special effort is made to order
  * the read with respect to pending writes going via the leader. Depending on the replication lag of the follower and
  * possible network partitions the value read may be very stale. Semantically such reads are equivalent to reading from
  * a cache which that has a timeout equal to the leader timeout off the followers in the cluster.
  *
  * This case class marks client work as safe to stale cached reads such that it is eligible to be optimised as a single
  * read if the cluster is configured to allow that.
  *
  * @param raw The actual raw client command which is opaque to Trex.
  */
case class OutdatableReadWork(val raw: AnyRef) extends OptimizableReadOnlyWork

/**
  * Placeholder currently not implemented.
  */
case class MembershipCommandValue(msgId: Long, members: Seq[Member]) extends CommandValue {
  override def bytes: Array[Byte] = emptyArray
}

object MemberStatus {
  def resolve(id: Int) = id match {
    case 0 => Learning
    case 1 => Accepting
    case 2 => Departed
  }
}

sealed trait MemberStatus {
  def id: Int
}

case object Learning extends MemberStatus {
  val id: Int = 0
}

case object Accepting extends MemberStatus {
  val id: Int = 1
}

case object Departed extends MemberStatus {
  val id: Int = 2
}

object Member {
  val pattern = "(.+):([0-9]+)".r
}

/**
  * Details of a member of the current paxos cluster.
 *
  * @param nodeUniqueId The unique paxos number for this membership
  * @param location     The location typically given as "host:port".
  * @param active       The status of the member.
  */
private[trex_paxos] case class Member(nodeUniqueId: Int, location: String, active: MemberStatus)

/**
  * A complete Paxos cluster.
 *
  * @param members The unique members of the cluster.
  */
private[trex_paxos] case class Membership(members: Seq[Member])

/**
  * Placeholder currently not implemented.
  *
  * Marking traffic as read-only allows for optimisation such as not forcing disk flushes and reading from replicas.
  */
sealed trait ReadOnlyCommand extends CommandValue

/**
  * Placeholder currently not implemented.
  *
  * An outdated read is a weak read of committed data directly from a replica. This can very likely be a stale read so
  * your application semantics must be safe to stale cached reads (e.g. any write based on the stale read are protected
  * by optimistic locking or compare-and-swap). This is the most scalable read type which may be suitable for high read
  * load with low update contention that is safe to stale reads.
  */
private[trex_paxos] case class OutdatedRead(msgId: Long, val bytes: Array[Byte]) extends ReadOnlyCommand

/**
  * Placeholder currently not implemented.
  *
  * A single read is a weak read of committed data ordered via the leader but possibly handed off to a follower. If
  * their is a leader failover (perhaps due to a partition) the read may be stale. As this can be stale read during such
  * failure scenarios your application semantics must be safe to stale cached reads (e.g. any write based on the stale
  * read are protected by optimistic locking or compare-and-swap). Given that under normal steady state the data wont
  * be stale this is suitable for high read loads with high update contention such as taking application leases using
  * compare-and-swap semantics.
  */
private[trex_paxos] case class SingleRead(msgId: Long, val bytes: Array[Byte]) extends ReadOnlyCommand

/**
  * Placeholder currently not implemented.
  *
  * A strong read is strictly ordered with respect to writes under all failure scenarios. Depending on cluster
  * configurations this may be by either arranged by either the use of a leader lease else by using a majority read.
  */
private[trex_paxos] case class StrongRead(msgId: Long, val bytes: Array[Byte]) extends ReadOnlyCommand
