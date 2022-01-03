package com.github.trex_paxos

import akka.actor.TypedActor.MethodCall
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, TypedActor, TypedProps}
import akka.serialization.SerializationExtension
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.refspec.RefSpecLike
import org.scalatest._
import matchers.should._

import scala.util.{Failure, Success, Try}

trait MethodCallTestTrait {
  def testMethod(testInput: String): String
}

class MethodCallTestClass extends MethodCallTestTrait {
  override def testMethod(testInput: String): String = {
    "out:" + testInput
  }
}

class MethodCallInvokingActor(target: Any) extends Actor with ActorLogging {

  val serialization = SerializationExtension(context.system)
  val serializer = serialization.serializerFor(classOf[MethodCall])

  def serialize(methodCall: MethodCall): Array[Byte] = {
    serializer.toBinary(methodCall)
  }

  def deserialize(bytes: Array[Byte]): MethodCall = {
    serializer.fromBinary(bytes, manifest = None).asInstanceOf[MethodCall]
  }

  override def receive: Receive = {
    case mc: MethodCall =>
      val bytes = serialize(mc)
      val methodCall = deserialize(bytes)
      log.debug(s"$methodCall")
      val method = methodCall.method
      val parameters = methodCall.parameters
      Try {
        Option(method.invoke(target, parameters: _*))
      } match {
        case Success(response) => response foreach {
          sender() ! _
        }
        case Failure(ex) =>
          log.error(ex, s"call to $method with ${parameters} got exception $ex")
          sender() ! ex
        case f => throw new IllegalArgumentException(f.toString)
      }
    case f => throw new IllegalArgumentException(f.toString)
  }
}

class MethodCallTests extends TestKit(ActorSystem("MethodCallTests"))
with RefSpecLike with ImplicitSender with BeforeAndAfterAll with Matchers {

  def `method call should serialize`: Unit = {

    val target = new MethodCallTestClass

    val invokingActor = system.actorOf(Props(classOf[MethodCallInvokingActor], target))

    val typedActor: MethodCallTestTrait =
      TypedActor(system).
        typedActorOf(
          TypedProps[MethodCallTestTrait](),
          invokingActor)

    val result = typedActor.testMethod("hello")

    result should be("out:hello")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
}
