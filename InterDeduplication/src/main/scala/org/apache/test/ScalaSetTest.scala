package org.apache.test

object ScalaSetTest {

  val usage =
    """
    Usage: spark-submit --master=yarn xx.jar [-h] [-p] <inputCsvPath> [-o] <outputLogPath> [-f] <testFuncName>
  """

  type OptionMap = Map[Symbol, String]


  def main(args: Array[String]): Unit = {

    var set1 = Set(2, 3, 5)
    val set2 = Set(1, 3, 6)
    set1 = set1.|(set2)
    println(set1.size)
  }

}
