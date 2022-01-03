package com.github.trex_paxos.internals

import argonaut._
import Argonaut._

object MemberPickle {

  implicit def MemberStatusCodecJson: CodecJson[MemberStatus.MemberStatus] = CodecJson({
    case MemberStatus.Accepting => "Accepting".asJson
    case MemberStatus.Learning => "Learning".asJson
  }, c => c.focus.string match {
    case Some("Accepting") => DecodeResult.ok(MemberStatus.Accepting)
    case Some("Learning") => DecodeResult.ok(MemberStatus.Learning)
    case _ => DecodeResult.fail("Could not decode MemberStatus", c.history)
  })


  implicit def VectorDecodeJson[A](implicit e: DecodeJson[A]): DecodeJson[Seq[A]] =
    DecodeJson( cursor => DecodeJson.ListDecodeJson[A].apply(cursor).map(list => list.toSeq))

  implicit def VectorEncodeJson[A](implicit e: EncodeJson[List[A]]): EncodeJson[Seq[A]] =
    EncodeJson(a => e(a.toList))

  implicit lazy val CodeMember: CodecJson[Member] =
    casecodec4(Member.apply, Member.unapply)("nodeUniqueId", "location", "clientLocation", "status")

  implicit lazy val CodeMembership: CodecJson[Membership] =
    casecodec2(Membership.apply, Membership.unapply)("name", "members")

  implicit lazy val CodecCommittedMembership: CodecJson[CommittedMembership] =
    casecodec2(CommittedMembership.apply, CommittedMembership.unapply)("name", "membership")

  def fromJson(jstring: String): Option[CommittedMembership] = {
    jstring.decodeOption[CommittedMembership]
  }

  def toJson(cm: CommittedMembership): String = {
    cm.asJson.toString()
  }
}


