package com.github.trex_paxos

import akka.actor.TypedActor.MethodCall
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, TypedActor, TypedProps}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, SpecLike}

import scala.util.{Try, Success, Failure}

trait MethodCallTestTrait {
  def testMethod(testInput: String): String
}

class MethodCallTestClass extends MethodCallTestTrait {
  override def testMethod(testInput: String): String = {
    "out:" + testInput
  }
}

class MethodCallInvokingActor(target: Any) extends Actor with ActorLogging {

  override def receive: Receive = {
    case mc: MethodCall =>
      val bytes = TrexExtension(context.system).serialize(mc)
      val methodCall = TrexExtension(context.system).deserialize(bytes)
      log.debug(s"$methodCall")
      val method = methodCall.method
      val parameters = methodCall.parameters
      Try {
        Option(method.invoke(target, parameters: _*))
      } match {
        case Success(response) => response foreach {
          sender ! _
        }
        case Failure(ex) =>
          log.error(ex, s"call to $method with ${parameters} got exception $ex")
          sender ! ex
      }
  }
}

class MethodCallTests extends TestKit(ActorSystem("MethodCallTests"))
with SpecLike with ImplicitSender with BeforeAndAfterAll with Matchers {

  def `method call should serialize`: Unit = {

    val target = new MethodCallTestClass

    val invokingActor = system.actorOf(Props(classOf[MethodCallInvokingActor], target))

    val typedActor: MethodCallTestTrait =
      TypedActor(system).
        typedActorOf(
          TypedProps[MethodCallTestTrait],
          invokingActor)

    val result = typedActor.testMethod("hello")

    result should be("out:hello")
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}
