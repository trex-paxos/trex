package com.github.trex_paxos.internals

import _root_.com.github.trex_paxos.library._
import _root_.com.github.trex_paxos.util.{Pickle, ByteChain}

/**
  * Binary pickling. I did try scala pickling but had all sorts of runtime problems including jvm crashes.
  * Note that RetransmitResponse could be big so in the future we might send a handle and
  * have the responder connect back and stream it one accept over time over TCP.
  */
object MemberPickle {

  import Pickle._

  val config: Map[Byte, (Class[_], Iterable[Byte] => AnyRef)] = Map(
   // 0xe.toByte -> (classOf[Membership] -> unpickleMembership _)
  )

  val toMap: Map[Class[_], Byte] = (config.map {
    case (b, (c, f)) => c -> b
  }).toMap

  val fromMap: Map[Byte, Iterable[Byte] => AnyRef] = config.map {
    case (b, (c, f)) => b -> f
  }

  val lengthOfInt = 4
  val lengthOfLong = 8
  val lengthOfBool = 1
  val sizeOfBallotNumber = 2 * lengthOfInt
  val sizeOfIdentifier = lengthOfInt + sizeOfBallotNumber + lengthOfLong
  val sizeOfProgress = sizeOfBallotNumber + sizeOfIdentifier

//  def pickleMember(m: Member) = {
//    pickleInt(m.nodeUniqueId) ++
//      pickleInt(m.location.getBytes("UTF8").length) ++
//      ByteChain(m.location.getBytes("UTF8")) ++
//      pickleInt(m.clientLocation.getBytes("UTF8").length) ++
//      ByteChain(m.clientLocation.getBytes("UTF8")) ++
//      pickleInt(m.active.id)
//  }
//
//  def unpickleMember(bytes: Iterable[Byte]): (Member, Iterable[Byte]) = {
//    val (idBs, r1) = bytes.splitAt(lengthOfInt)
//    val id = unpickleInt(idBs)
//    val (sl1Bs, r2) = r1.splitAt(lengthOfInt)
//    val l1 = unpickleInt(sl1Bs)
//    val (location, r3) = r2.splitAt(l1)
//    val (sl2Bs, r4) = r3.splitAt(lengthOfInt)
//    val l2 = unpickleInt(sl2Bs)
//    val (clientLocation, r5) = r4.splitAt(l2)
//    val (stateBs, rest) = r5.splitAt(lengthOfInt)
//    val state = unpickleInt(stateBs)
//    (Member(id, new String(location.toArray, "UTF8"),
//      new String(clientLocation.toArray, "UTF8"), MemberStatus.resolve(state)), rest)
//  }

//  def pickleMembershipValue(m: MembershipCommandValue) = pickleStringUtf8(m.msgId) ++ pickleMembership(m.membership)
//
//  def unpickleMembershipValue(b: ByteChain): MembershipCommandValue = {
//    val (idl, idmsg) = b.splitAt(lengthOfInt)
//    val idlength = unpickleInt(idl)
//    val msgId = new String(idmsg.take(idlength).toArray, "UTF8")
//    val (id, membership) = idmsg.splitAt(idlength)
//    MembershipCommandValue(msgId, unpickleMembership(membership))
//  }

//  def pickleMembership(m: Membership) =  pickleInt(m.name.getBytes("UTF8").length) ++ ByteChain(m.name.getBytes("UTF8")) ++
//    pickleInt(m.members.size) ++
//    m.members.foldLeft(ByteChain.empty)((bs, m) => bs ++ pickleMember(m))
//
//  def unpickleMembership(b: Iterable[Byte]): Membership = {
//    val (sb, rb) = b.splitAt(lengthOfInt)
//    val length = unpickleInt(sb)
//    val (name,rb2) = rb.splitAt(length)
//    val (size, remainder) = rb2.splitAt(lengthOfInt)
//    val s = unpickleInt(size)
//    def take(b: Iterable[Byte], members: Seq[Member], count: Int): Seq[Member] = {
//      count match {
//        case 0 =>
//          members
//        case _ =>
//          val (member, remainder) = unpickleMember(b)
//          take(remainder, members :+ member, count - 1)
//      }
//    }
//    val members = take(remainder, Seq.empty, s)
//    Membership(new String(name.toArray, "UTF8"), members)
//  }

//  def pickleMembershipQuery(m: MembershipQuery): ByteChain = pickleLong(m.slot)
//
//  def unpickleMembershipQuery(b: Iterable[Byte]): MembershipQuery = MembershipQuery(unpickleLong(b))

  def pickle(a: AnyRef): ByteChain = a match {
//    case m: MembershipCommandValue => pickleMembershipValue(m)
    //case m: Membership => pickleMembership(m)
   // case m: Member => pickleMember(m)
//    case m: MembershipQuery => pickleMembershipQuery(m)
    case x =>
      System.err.println(s"don't know how to pickle $x so returning empty ByteChain")
      ByteChain.empty
  }

  def unpack(b: Iterable[Byte]): AnyRef = fromMap(b.head)(b.tail)

  def pack(a: AnyRef): ByteChain = ByteChain(Array(toMap(a.getClass))) ++ pickle(a)
}