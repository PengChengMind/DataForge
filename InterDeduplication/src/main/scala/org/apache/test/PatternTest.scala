package org.apache.test

import java.io.File

import scala.io.Source

object PatternTest {

  def main(args: Array[String]): Unit = {
      val pattern = """[^\u4e00-\u9fa5]""".r
      val string = "你好达大幅度发134234hellohi_good---dfa?"

      println(pattern.replaceAllIn(string, ""))


    val str = "HHHHello我们"
    val p2 = "[A-Z]".r
    println(str.replaceAll("([A-Z])", "$14"))
    println(p2.findAllIn(str).toList.size)
    println(pattern.findAllIn(str).mkString(""))

    val str3 = "нумар — Адміністрацыйны цэнтрam";
    val pattern3 = """[\pP\pS]""".r
    println("str3:" + str3)
    println("st_3:" + pattern3.replaceAllIn(str3, ""))
    val pattern4 = """\d+""".r
    println("contains: " + (pattern4.findFirstIn(str3) == None))

    val file = new File("input/page_3000/question-68342062")
    val head = Source.fromFile(file).getLines().next()
    println(Source.fromFile(file).getLines().next().contains("body"))

  }

}
