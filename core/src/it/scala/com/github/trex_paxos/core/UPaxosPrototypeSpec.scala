package com.github.trex_paxos.core

import akka.actor.{ActorRef, ActorSystem}
import akka.event.{LogSource, Logging}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.github.trex_paxos._
import com.github.trex_paxos.library._
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.scalatest.refspec.RefSpecLike

import scala.collection.immutable.{Set, SortedMap, TreeMap}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.collection.mutable.ArrayBuffer

object UPaxosPrototypeSpec {
  val config = _root_.com.typesafe.config.ConfigFactory.parseString("trex.leader-timeout-min=1\ntrex.leader-timeout-max=10\nakka.loglevel = \"DEBUG\"")
}

class TestUPaxosActor(config: PaxosProperties, clusterSizeF: () => Int, nodeUniqueId: Int, broadcastRef: ActorRef, journal: Journal, override val delivered: ArrayBuffer[CommandValue], tracer: Option[AkkaPaxosActor.Tracer])
  extends TestAkkaPaxosActorNoTimeout(config, clusterSizeF, nodeUniqueId, broadcastRef, journal, delivered, tracer) with PaxosLenses {

  val memberStore = new MemberStore {
    var eras: SortedMap[Int, Era] = SortedMap()

    override def saveMembership(era: Era): Unit = {
      if (eras.keySet.size > 0) require(era.era == eras.keySet.last + 1)
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
          // persist the committed membership
          setMembership(committedMembership)
          logger.info("new committed membership at era {} and slot {} is {}", era, slot, membership)
          // check if this current node is the leader that send the membership which has been comitted
          val optionEra = paxosAgent.data.epoch match {
            case Some(ballotNumber) =>
              number match {
                case `ballotNumber` =>
                  // perform UPaxos upgrade of ballot number to the latest era
                  val newEraPrepare = UPaxos.upgradeBallotNumberToNewEra(nodeUniqueId, ballotNumber, paxosAgent.data.progress)
                  val newEraOverlap = UPaxos.computeLeaderOverlap(nodeUniqueId, committedMembership)
                  uPaxosContext = Option(UPaxosContext(newEraPrepare, newEraOverlap))
                  val outMessages = Routing.route(nodeUniqueId, newEraPrepare, newEraOverlap.prepareNodes)
                  logger.info("overlap prepare messages are {}", outMessages)
                  outMessages.foreach(send(sender(), _))
                  Option(newEraPrepare.id.number.era)
              }
            case None => logger.error("should be unreachable")
              None
          }
          optionEra map { e => MemberPickle.toJson(Era(e, committedMembership)).getBytes("UTF8") }
      }
  }

  def currentMembership = memberStore.loadMembership().getOrElse(throw new IllegalArgumentException("uninitialised memberstore"))

  def currentBroadcastNodes(paxosMessage: PaxosMessage): Iterable[Int] = {
    currentMembership.membership.nodes.map(_.nodeIdentifier)
  }

  override def receive: Receive = {
    case m: PaxosMessage =>

      def notNewEraPrepareWork() = {
        val event = new PaxosEvent(this, paxosAgent, m)
        val agent = paxosAlgorithm(event)
        trace(event, sender().toString(), sent)
        val outMessages = this.sent flatMap {
          case m: PaxosMessage => Routing.route(nodeUniqueId, m, currentBroadcastNodes(m))
        }
        outMessages foreach {
          case o@OutboundMessage(_, a: Accept) =>
            uPaxosContext match {
              case Some(UPaxosContext(_, overlap, _)) =>
                send(sender(), o.copy(targets = overlap.acceptNodes.toSet))
              case _ =>
                send(sender(), o)
            }
          case o =>
            send(sender(), o)
        }
        this.sent = collection.immutable.Seq()
        clearLeaderOverlapWorkIfLostLeadership(paxosAgent.role, agent.role)
        paxosAgent = agent
      }

      uPaxosContext match {
        case None =>
          notNewEraPrepareWork()
        case Some(UPaxosContext(prepare, overlap, _)) =>
          m match {
            case ackOrNack: PrepareResponse =>
              if (ackOrNack.requestId == prepare.id) {
                handleEraPrepareResponse(nodeUniqueId, overlap.prepareNodes, currentMembership.membership, ackOrNack) match {
                  case None => // no outcome yet so nothing to do
                  case Some(true) => // outcome success
                    // process our own prepare message to upgrade our own promise and progress
                    if (prepare.id.number > paxosAgent.data.progress.highestPromised) {
                      val data = progressLens.set(paxosAgent.data, Progress.highestPromisedLens.set(paxosAgent.data.progress, prepare.id.number))
                      // journal promise
                      journal.saveProgress(data.progress)
                      // upgrade our epoch to our promise so that our new accept message go out with the higher number
                      paxosAgent = paxosAgent.copy(data = epochLens.set(data, Option(data.progress.highestPromised)))
                      // tell the accept nodes about the new prepare. this isnt needed for safety only the aesthetics of symmetry of state between nodes
                      send(sender(), OutboundMessage(overlap.acceptNodes.toSet, prepare))
                      logger.info("succeed in upgrading to new era ballot number {}", paxosAgent.data.progress.highestPromised)
                    }
                    reset()
                  case Some(false) =>
                    logger.warning("failed to upgrade to new era ballot number clearing overlap state")
                    reset()

                }
              } else {
                notNewEraPrepareWork()
              }
            case _ =>
              notNewEraPrepareWork()
          }
      }

    case f =>
      val err = s"Node ${nodeUniqueId} received unknown messages type ${f.getClass.getCanonicalName} : ${f}"
      logger.error(err)
      throw new IllegalArgumentException(err)
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

    // TODO strip this out as all of this is tested in other classes
    def threeNodesSteadyState(actor0: TestActorRef[TestUPaxosActor], actor1: TestActorRef[TestUPaxosActor], actor2: TestActorRef[TestUPaxosActor]) = {
      expectNoMsg(25 millisecond)
      // when node zero times-out
      actor0 ! CheckTimeout
      // it issues a low prepare
      expectMsg(50 millisecond, OutboundMessage(Set(1, 2), minPrepare))
      // and node one will nack the load prepare
      actor1 ! minPrepare
      val nack1: PrepareNack = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, p: PrepareNack) if set == Set(0) =>
          p
        case f => fail(f.toString)
      }
      nack1.requestId.from should be(0)
      // and node two will nack the load prepare
      actor2 ! minPrepare
      val nack2: PrepareNack = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, p: PrepareNack) if set == Set(0) =>
          p
        case f => fail(f.toString)
      }
      nack2.requestId.from should be(0)
      // which will cause node zero to issue a higher prepare
      // when we send it back to node zero
      actor0 ! nack1
      // it issues a higher prepare
      val phigh: Prepare = expectMsgPF(50 milliseconds) {
        case OutboundMessage(set, hprepare: Prepare) if set == Set(1, 2) => hprepare
        case f => fail(f.toString)
      }
      phigh.id.logIndex should be(1)
      phigh.id.number.nodeIdentifier should be(0)

      // when we send that high prepare to node one
      actor1 ! phigh

      // it should ack
      val pack1 = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, p: PrepareAck) if set == Set(0) => p
        case f => fail(f.toString)
      }
      pack1.requestId should be(phigh.id)

      // send it to node two it should also ack
      actor2 ! phigh
      // it should ack
      val pack2 = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, p: PrepareAck) if set == Set(0) => p
        case f => fail(f.toString)
      }
      pack2.requestId should be(phigh.id)

      // when we send that back to node zero
      actor0 ! pack1

      // it will issue a noop accept
      val accept: Accept = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, a: Accept) if set == Set(1, 2) => a
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
        case OutboundMessage(set, a: AcceptAck) if set == Set(0) => a
        case f => fail(f.toString)
      }
      aack1_1.requestId should be(accept.id)

      // node two will also ack
      actor2 ! accept

      // it will ack
      val aack1_2: AcceptAck = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, a: AcceptAck) if set == Set(0) => a
        case f => fail(f.toString)
      }
      aack1_2.requestId should be(accept.id)

      // when we send an ack to node zero
      actor0 ! aack1_1
      // it commits the noop
      expectMsgPF(50 millisecond) {
        case OutboundMessage(set, c: Commit) if set == Set(1, 2) => // good
        case f => fail(f.toString)
      }
      // then send it some data

      actor0 ! hw
      // it will send out an accept
      val accept2 = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, a: Accept) if set == Set(1, 2) => a
        case f => fail(f.toString)
      }
      accept2.id.logIndex should be(2)
      accept2.value.asInstanceOf[ClientCommandValue].bytes.length should be(1)
      accept2.id.number should be(phigh.id.number)
      // when we send that to node one
      actor1 ! accept2
      // it will ack
      val aack2_1 = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, a: AcceptAck) if set == Set(0) => a
        case f => fail(f.toString)
      }
      aack2_1.requestId should be(accept2.id)

      // also node two will ack
      actor2 ! accept2
      // it will ack
      val aack2_2 = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, a: AcceptAck) if set == Set(0) => a
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
        case OutboundMessage(set, c: Commit) if set == Set(1, 2) => c
        case f => fail(f.toString)
      }
      // send that to node one and two for them to delivery the value

      actor1 ! commit
      actor2 ! commit
    }


    val node0 = Node(0, Addresses(Address("localhost", 1), Address("localhost", 2)))
    val node1 = Node(1, Addresses(Address("localhost", 2), Address("localhost", 3)))
    val node2 = Node(2, Addresses(Address("localhost", 4), Address("localhost", 4)))

    val nodes = Set(node0, node1, node2)

    // typical weights for a 3 node cluster
    val originalQuorum = Quorum(2, Set(Weight(0, 1), Weight(1, 1), Weight(2, 1)))

    // original membership is in effect from the zeroth slow
    val originalMembership = Membership(quorumForPromises = originalQuorum, quorumForAccepts = originalQuorum, nodes = nodes, effectiveSlot = Some(0))

    // doubled weights which is the first step in swapping out a single node
    val newQuorum = Quorum(4, Set(Weight(0, 2), Weight(1, 2), Weight(2, 2)))

    // the numer membership is the same set of nodes but with doubled weights
    val newMembership = Membership(quorumForPromises = newQuorum, quorumForAccepts = newQuorum, nodes = nodes, effectiveSlot = None)

    def `should compute overlap unweighted membership`: Unit = {
      val LeaderOverlap(prepares, accepts) = UPaxos.computeLeaderOverlap(0, originalMembership)

      prepares.toSet shouldBe Set(1)
      accepts.toSet shouldBe Set(2)
    }

    def `should compute overlap double weighted membership`: Unit = {
      val LeaderOverlap(prepares, accepts) = UPaxos.computeLeaderOverlap(0, newMembership)

      prepares.toSet shouldBe Set(1)
      accepts.toSet shouldBe Set(2)
    }

    def `should compute outcomes no votes`: Unit = {
      val prepareNodes: Iterable[Int] = Set(1)

      UPaxos.computeNewEraPrepareOutcome(0, prepareNodes, originalQuorum, SortedMap()) match {
        case None => // success
        case f => fail(f.toString)
      }
    }

    import Ordering._

    val lowValue = 1

    val initialData = PaxosData(progress = Progress(
      highestPromised = BallotNumber(lowValue, lowValue),
      highestCommitted = Identifier(from = 0, number = BallotNumber(lowValue, lowValue), logIndex = 0)
    ), leaderHeartbeat = 0, timeout = 0, prepareResponses = TreeMap(), epoch = None, acceptResponses = TreeMap())

    val recoverHighPrepare = Prepare(Identifier(0, BallotNumber(lowValue + 1, 0), 1L))

    def `should compute outcome one negative votes`: Unit = {

      val prepareNodes: Iterable[Int] = Set(1)
      val pNack = PrepareNack(recoverHighPrepare.id, 0, initialData.progress, 0, 0)

      UPaxos.computeNewEraPrepareOutcome(0, prepareNodes, originalQuorum, SortedMap(1 -> pNack)) match {
        case Some(false) => // success
        case f => fail(f.toString)
      }
    }

    def `should compute outcome one positive votes`: Unit = {

      val prepareNodes: Iterable[Int] = Set(1)
      val pAck = PrepareAck(recoverHighPrepare.id, 0, initialData.progress, 0, 0, None)

      UPaxos.computeNewEraPrepareOutcome(0, prepareNodes, originalQuorum, SortedMap(1 -> pAck)) match {
        case Some(true) => // success
        case f => fail(f.toString)
      }
    }

    def `should perform UPaxos reconfiguration` {

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

      leader ! ClusterCommandValue("reconfig", MemberPickle.toJson(Era(-1, newMembership)).getBytes("UTF8"))

      // it will send out an accept
      val accept1 = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, a: Accept) if set == Set(1, 2) => a
        case f => fail(f.toString)
      }

      follower1 ! accept1
      // it will ack
      val aack1_1 = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, a: AcceptAck) if set == Set(0) => a
        case f => fail(f.toString)
      }

      follower2 ! accept1
      // it will ack
      expectMsgPF(50 millisecond) {
        case OutboundMessage(set, a: AcceptAck) if set == Set(0) => a
        case f => fail(f.toString)
      }

      // send the ack to the leader
      leader ! aack1_1

      // leader will issue the overlap prepare for new era
      val prepareNewEra = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, p: Prepare) if set == Set(1) => p
        case f => fail(f.toString)
      }

      prepareNewEra.id.number.era shouldBe aack1_1.requestId.number.era + 1
      prepareNewEra.id.number.counter shouldBe aack1_1.requestId.number.counter
      prepareNewEra.id.number.nodeIdentifier shouldBe aack1_1.requestId.number.nodeIdentifier

      // leader should return the committed membership with the new era to the client
      val newEra = expectMsgPF(50 millisecond) {
        case Some(byte: Array[Byte]) => MemberPickle.fromJson(new String(byte, "UTF8"))
        case f => fail(f.toString)
      }

      newEra match {
        case Some(era: Era) => era.era shouldBe 1
        case f => fail(f.toString)
      }

      // leader should notify followers of the commit
      val c1 = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, c: Commit) if set == Set(1, 2) => c
        case f => fail(f.getClass.getCanonicalName + " " + f.toString)
      }

      // if we send another client value
      leader ! hw

      // it will send an accept only to follower2 in the accept partition using the old era
      val oldEraAccept = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, a: Accept) if set == Set(2) && a.id.number.era == prepareNewEra.id.number.era - 1 =>
          a
        case f =>
          fail(f.toString)
      }

      // that will be accepted by follower2
      follower2 ! oldEraAccept

      val oldEraAcceptAck = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, a: AcceptAck) if set == Set(0) && a.from == 2 =>
          a
        case f =>
          fail(f.toString)
      }

      // when the leader sees the old era ack
      leader ! oldEraAcceptAck

      // the leader will deliver back to the client
      expectMsgPF(50 millisecond) {
        case b: Array[Byte] => // good
        case f =>
          fail(f.toString)
      }

      // and commit that message to all nodes
      // TODO this would cause Follower1 to ask for a retransmission of what it missed should speculatively send values
      expectMsgPF(50 millisecond) {
        case OutboundMessage(set, c: Commit) if set == Set(1, 2) && c.identifier == oldEraAccept.id => // good
        case f =>
          fail(f.toString)
      }

      // when we send the new era prepare to follower1 in the prepare partition using the new era
      follower1 ! prepareNewEra

      val newEraPrepareAck = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, p: PrepareAck) if set == Set(0) =>
          p.from shouldBe 1
          p.requestId shouldBe prepareNewEra.id
          p
        case f => fail(f.toString)
      }

      // when we send that ack to the leader
      leader ! newEraPrepareAck

      // leader will bring the other node into consistency (only to make the state symmetric within the cluster)
      val newEraPrepare2 = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, p: Prepare) if set == Set(2) && p.id == prepareNewEra.id => p
        case f => fail(f.toString)
      }

      follower2 ! newEraPrepare2

      expectMsgPF(50 millisecond) {
        case OutboundMessage(set, p: PrepareAck) if set == Set(0) =>
          p.from shouldBe 2
          p.requestId shouldBe prepareNewEra.id
          p
        case f => fail(f.toString)
      }

      // if we send another client value
      leader ! hw

      // it will send an accept to all nodes using the higher number
      val newEraAccept = expectMsgPF(50 millisecond) {
        case OutboundMessage(set, a: Accept) if set == Set(1, 2) && a.id.number == prepareNewEra.id.number => // good
        case f =>
          fail(f.toString)
      }

    }
  }

}
