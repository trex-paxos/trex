package com.github.simbo1905.trex.internals


/**
 * Client request command has an id to correlate to the server response.
 */
case class ClientRequestCommandValue(msgId: Long, val bytes: Array[Byte]) extends CommandValue

/**
 *
 * @param id The id of the ClientRequestCommandValue being responded to.
 * @param response The
 */
case class ServerResponse(id: Long, val response: Option[AnyRef])

case object NoOperationCommandValue extends CommandValue {
  def bytes = emptyArray

  val msgId = -1L
}

case class MembershipCommandValue(msgId: Long, members: Seq[ClusterMember]) extends CommandValue {
  override def bytes: Array[Byte] = emptyArray
}

case class ClusterMember(nodeUniqueId: Int, location: String, active: Boolean)

case class Membership(members: Seq[ClusterMember])