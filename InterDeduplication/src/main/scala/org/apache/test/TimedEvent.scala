package org.apache.test

import java.util.Timer
import java.util.TimerTask

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{Failure, Success}

object TimedEvent {



  def main(args: Array[String]): Unit = {
    println("main begin")

    val test = Future {
      println("test")
      Thread.sleep(3000)
      250
    }

    val result = Promise[Int]
    test.onComplete(x => x match {
      case Success(v1) => println("v1 = " + v1); result.success(v1)
      case Failure(t) => println("failed"); result.failure(t)
    })
    result.future
    Thread.sleep(5000)
    println("main end")
  }
}
