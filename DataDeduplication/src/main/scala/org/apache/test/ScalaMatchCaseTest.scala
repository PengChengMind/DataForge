package org.apache.test

object ScalaMatchCaseTest {

  def main(args: Array[String]): Unit = {
    val p = 16
    p match {
      case 16 => println("match 5")
      case 16 => println("match 4")
    }
  }

}
