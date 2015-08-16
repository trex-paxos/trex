package com.github.simbo1905.trex

import akka.actor.TypedActor.MethodCall
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, TypedActor, TypedProps}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, SpecLike}

import scala.util.control.NonFatal

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
      try {
        val response = method.invoke(target, parameters: _*)
        if (response != null) sender ! response
      } catch {
        case NonFatal(ex) =>
          log.error(ex, s"call to $method with ${parameters} got exception $ex")
          sender ! ex
      }
  }
}

class MethodCallTests extends TestKit(ActorSystem("MethodCallTests", ConfigFactory.parseString("akka.extensions = [\"com.github.simbo1905.trex.TrexExtension\"]")))
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
