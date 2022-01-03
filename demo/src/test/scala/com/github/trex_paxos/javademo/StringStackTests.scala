package com.github.trex_paxos.javademo

import java.io.File

import org.scalatest.refspec.RefSpec
import org.scalatest._
import matchers.should._

class StringStackTests extends RefSpec with Matchers {

  object `A StringStack` {

    object `when empty` {

      object `should push, peek and pop` {
        val stack: StringStack = new StringStackImpl
        stack.empty() shouldBe true
        stack.push("hello")
        stack.empty() shouldBe false
        val h = stack.peek()
        stack.empty() shouldBe false
        h shouldBe "hello"
        val h2 = stack.pop()
        stack.empty() shouldBe true
        h2 shouldBe "hello"
      }

      object `should multi push and pop` {
        val stack: StringStack = new StringStackImpl
        stack.push("hello")
        stack.push("world")
        val w = stack.pop()
        val h = stack.pop()
        w shouldBe "world"
        h shouldBe "hello"
      }

      object `should retain state` {
        val file = File.createTempFile("stack", "data")

        def one(): Unit = {
          val stack = new StringStackImpl(file)
          stack.empty() shouldBe true
          stack.push("hello2")
          stack.push("world2")
        }

        one()

        def two(): Unit = {
          val other = new StringStackImpl(file)
          other.empty() shouldBe false
          val w = other.pop()
          w shouldBe "world2"
          val h = other.pop()
          h shouldBe "hello2"
        }

        two()

        val empty = new StringStackImpl(file)
        empty.empty() shouldBe true
      }

      object `pop should only pop one` {
        val file = File.createTempFile("stack", "data")

        def one(): Unit = {
          val stack = new StringStackImpl(file)
          stack.empty() shouldBe true
          stack.push("helloA")
          stack.push("worldA")
        }

        one()

        def two(): Unit = {
          val other = new StringStackImpl(file)
          other.empty() shouldBe false
          val w = other.pop()
          w shouldBe "worldA"
        }

        two()

        def three(): Unit = {
          val other = new StringStackImpl(file)
          other.empty() shouldBe false
          val w = other.pop()
          w shouldBe "helloA"
        }

        three()

        val empty = new StringStackImpl(file)
        empty.empty() shouldBe true
      }

    }

  }

}
