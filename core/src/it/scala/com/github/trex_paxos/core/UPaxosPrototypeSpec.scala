package com.github.trex_paxos.core

import akka.actor.{ActorRef, ActorSystem}
import akka.event.{LogSource, Logging}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.github.trex_paxos._
import com.github.trex_paxos.library._
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.scalatest.refspec.RefSpecLike

import scala.collection.immutable.SortedMap
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.collection.mutable.ArrayBuffer

object UPaxosPrototypeSpec {
  val config = _root_.com.typesafe.config.ConfigFactory.parseString("trex.leader-timeout-min=1\ntrex.leader-timeout-max=10\nakka.loglevel = \"DEBUG\"")
}

class TestUPaxosActor(config: PaxosProperties, clusterSizeF: () => Int, nodeUniqueId: Int, broadcastRef: ActorRef, journal: Journal, override val delivered: ArrayBuffer[CommandValue], tracer: Option[AkkaPaxosActor.Tracer])
  extends TestAkkaPaxosActorNoTimeout(config, clusterSizeF, nodeUniqueId, broadcastRef, journal, delivered, tracer) {

  val memberStore = new MemberStore {
    var eras: SortedMap[Int, Era] = SortedMap()

    override def saveMembership(era: Era): Unit = {
      if( eras.keySet.size > 0 ) require(era.era == eras.keySet.last + 1)
      eras = eras + (era.era -> era)
    }

    override def loadMembership(): Option[Era] = {
      eras.lastOption.map(_._2)
    }
  }

  def setMembership(membership: Membership) = {
    membership.effectiveSlot match {
      case None => throw new IllegalArgumentException(membership.toString)
      case Some(slot) =>
        val lastEra = memberStore.loadMembership match {
          case None => 0
          case Some(era) => era.era
        }
        memberStore.saveMembership(Era(lastEra + 1, membership))
    }
  }

  override val deliverMembership: PartialFunction[Payload, Any] = {
    case Payload(Identifier(_, number, slot), ClusterCommandValue(_, bytes)) =>
      val json = new String(bytes, "UTF8")
      val era: Option[Era] = MemberPickle.fromJson(json)
      logger.info("deliverMembership: {}", era)
      era match {
        case None => throw new IllegalArgumentException(json)
        case Some(Era(e, membership)) =>
          // set the slot as it is now committed
          val committedMembership = membership.copy(effectiveSlot = Some(slot))
          setMembership(committedMembership)
          logger.info("new committed membership at era {} and slot {} is {}", era, slot, membership)
          // check if this current node is the leader that send the membership which has been comitted
          paxosAgent.data.epoch match {
            case Some(ballotNumber) =>
              number match {
                case `ballotNumber` =>
                  // perform UPaxos upgrade of ballot number to the latest era
                  newEraBallotNumber = Some(ballotNumber.copy())
              }
            case None => logger.error("should be unreachable")
          }
      }
  }

  override def receive: Receive = {
    case m: PaxosMessage =>
      val event = new PaxosEvent(this, paxosAgent, m)
      val agent = paxosAlgorithm(event)
      trace(event, sender().toString(), sent)
      transmit(sender())
      paxosAgent = agent
    case f => logger.error("Received unknown messages type {}", f)
  }

}

class UPaxosPrototypeSpec extends TestKit(ActorSystem("UPaxosPrototypeSpec",
  InteractionSpec.config)) with RefSpecLike with ImplicitSender with BeforeAndAfterAll with Matchers {

  implicit val myLogSourceType: LogSource[UPaxosPrototypeSpec] = new LogSource[UPaxosPrototypeSpec] {
    def genString(a: UPaxosPrototypeSpec) = "UPaxosPrototypeSpec"
  }

  val logger = Logging(system, this)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val minPrepare = Prepare(Identifier(0, BallotNumber(0, 0), 0))

  object `A three node UPaxos cluster` {

    val hw = ClientCommandValue("0", Array[Byte](1))

    def threeNodesSteadyState(actor0: TestActorRef[TestUPaxosActor], actor1: TestActorRef[TestUPaxosActor], actor2: TestActorRef[TestUPaxosActor]) = {
      expectNoMsg(25 millisecond)
      // when node zero times-out
      actor0 ! CheckTimeout
      // it issues a low prepare
      expectMsg(50 millisecond, minPrepare)
      // and node one will nack the load prepare
      actor1 ! minPrepare
      val nack1: PrepareNack = expectMsgPF(50 millisecond) {
        case p: PrepareNack => p
        case f => fail(f.toString)
      }
      nack1.requestId.from should be(0)
      // and node two will nack the load prepare
      actor2 ! minPrepare
      val nack2: PrepareNack = expectMsgPF(50 millisecond) {
        case p: PrepareNack => p
        case f => fail(f.toString)
      }
      nack2.requestId.from should be(0)
      // which will cause node zero to issue a higher prepare
      // when we send it back to node zero
      actor0 ! nack1
      // it issues a higher prepare
      val phigh: Prepare = expectMsgPF(50 milliseconds) {
        case hprepare: Prepare => hprepare
        case f => fail(f.toString)
      }
      phigh.id.logIndex should be(1)
      phigh.id.number.nodeIdentifier should be(0)
      // when we send that high prepare to node one
      actor1 ! phigh
      // it should ack
      val pack1 = expectMsgPF(50 millisecond) {
        case p: PrepareAck => p
        case f => fail(f.toString)
      }
      pack1.requestId should be(phigh.id)
      // send it to node two it should also ack
      actor2 ! phigh
      // it should ack
      val pack2 = expectMsgPF(50 millisecond) {
        case p: PrepareAck => p
        case f => fail(f.toString)
      }
      pack2.requestId should be(phigh.id)

      // when we send that back to node zero
      actor0 ! pack1

      // it will issue a noop accept
      val accept: Accept = expectMsgPF(50 millisecond) {
        case a: Accept => a
        case f => fail(f.toString)
      }

      accept.id.logIndex should be(1)
      accept.value shouldBe NoOperationCommandValue
      accept.id.number should be(phigh.id.number)
      // and ack its own accept
      actor0.underlyingActor.data.acceptResponses match {
        case map if map.nonEmpty =>
          map.get(accept.id) match {
            case None => fail()
            case Some(AcceptResponsesAndTimeout(_, _, responses)) =>
              responses.values.headOption match {
                case Some(a: AcceptAck) => //good
                case x => fail(x.toString)
              }
          }
        case x => fail(x.toString)
      }
      // when we send that to node one
      actor1 ! accept

      // it will ack
      val aack1_1: AcceptAck = expectMsgPF(50 millisecond) {
        case a: AcceptAck => a
        case f => fail(f.toString)
      }
      aack1_1.requestId should be(accept.id)

      // node two will also ack
      actor2 ! accept

      // it will ack
      val aack1_2: AcceptAck = expectMsgPF(50 millisecond) {
        case a: AcceptAck => a
        case f => fail(f.toString)
      }
      aack1_2.requestId should be(accept.id)

      // when we send an ack to node zero
      actor0 ! aack1_1
      // it commits the noop
      expectMsgPF(50 millisecond) {
        case c: Commit => // good
        case f => fail(f.toString)
      }
      // then send it some data

      actor0 ! hw
      // it will send out an accept
      val accept2 = expectMsgPF(50 millisecond) {
        case a: Accept => a
        case f => fail(f.toString)
      }
      accept2.id.logIndex should be(2)
      accept2.value.asInstanceOf[ClientCommandValue].bytes.length should be(1)
      accept2.id.number should be(phigh.id.number)
      // when we send that to node one
      actor1 ! accept2
      // it will ack
      val aack2_1 = expectMsgPF(50 millisecond) {
        case a: AcceptAck => a
        case f => fail(f.toString)
      }
      aack2_1.requestId should be(accept2.id)

      // also node two will ack
      actor2 ! accept2
      // it will ack
      val aack2_2 = expectMsgPF(50 millisecond) {
        case a: AcceptAck => a
        case f => fail(f.toString)
      }
      aack2_2.requestId should be(accept2.id)

      // when we send the first ack back to node zero
      actor0 ! aack2_1
      // then it responds with the committed work
      expectMsgPF(50 millisecond) {
        case b: Array[Byte] if b(0) == -1 => true
        case b => fail(s"$b")
      }
      // it will send out a commit
      val commit: Commit = expectMsgPF(50 millisecond) {
        case c: Commit => c
        case f => fail(f.toString)
      }
      // when we send that to node one and two
      actor1 ! commit
      actor2 ! commit

    }

    def `should perform UPaxos reconfiguration` {

      val node0 = Node(0, Addresses(Address("localhost", 1), Address("localhost", 2)))
      val node1 = Node(1, Addresses(Address("localhost", 2), Address("localhost", 3)))
      val node2 = Node(2, Addresses(Address("localhost", 4), Address("localhost", 4)))

      val nodes = Set(node0, node1, node2)

      // typical weights for a 3 node cluster
      val originalQuorum = Quorum(2, Set(Weight(0, 1), Weight(1, 1), Weight(2, 1)))

      // original membership is in effect from the zeroth slow
      val originalMembership = Membership(quorumForPromises = originalQuorum, quorumForAccepts = originalQuorum, nodes = nodes, effectiveSlot = Some(0))

      // doubled weights which is the first step in swapping out a single node
      val newQuorum = Quorum(2, Set(Weight(0, 2), Weight(1, 2), Weight(2, 2)))

      // the numer membership is the same set of nodes but with doubled weights
      val newMembership = Membership(quorumForPromises = originalQuorum, quorumForAccepts = originalQuorum, nodes = nodes, effectiveSlot = None)

      // given node zero
      val journal0 = new TestJournal
      val leader = TestActorRef(new TestUPaxosActor(PaxosProperties(InteractionSpec.config), () => 3, 0, self, journal0, ArrayBuffer.empty, None))
      leader.underlyingActor.setMembership(originalMembership)

      // and node one
      val journal1 = new TestJournal
      val follower1 = TestActorRef(new TestUPaxosActor(PaxosProperties(InteractionSpec.config), () => 3, 1, self, journal1, ArrayBuffer.empty, None))
      follower1.underlyingActor.setMembership(originalMembership)

      // and node two
      val journal2 = new TestJournal
      val follower2 = TestActorRef(new TestUPaxosActor(PaxosProperties(InteractionSpec.config), () => 3, 2, self, journal2, ArrayBuffer.empty, None))
      follower2.underlyingActor.setMembership(originalMembership)

      threeNodesSteadyState(leader, follower1, follower2)

      // and both nodes will have delivered the value
      Seq(journal0, journal1, journal2).map(_._map().get(2).getOrElse(fail).value) should be(Seq(hw, hw, hw))

      leader.underlyingActor.paxosAgent.role shouldBe (Leader)
      follower1.underlyingActor.paxosAgent.role shouldBe (Follower)
      follower2.underlyingActor.paxosAgent.role shouldBe (Follower)


      //      leader ! ClusterCommandValue("reconfig", )
    }
  }

}
