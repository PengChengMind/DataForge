package org.apache.test

import com.huaban.analysis.jieba.JiebaSegmenter
import component.DynamicParameter
import tokenizer.{JiebaTokenizer, EnTokenizer}

object testJieba {

  def main(args: Array[String]): Unit = {
    val jiebaSegmenter = new JiebaSegmenter
    val str1 = "县府与林务局今年预计在乌崁、成功水库、铁线、兴仁水库、双湖园区、白沙等地，进行林相更新与新植造林";
    val str2 = "县府与林务局今年预计在乌崁成功水库铁线兴仁水库双湖园区白沙等地进行林相更新与新植造林"

    val result1 = jiebaSegmenter.sentenceProcess(str1)
    val result2 = jiebaSegmenter.sentenceProcess(str2)
    println(result1)
    println(result2)


    println("Сёлета ён праводзіўся другі раз.".length)
  }

}
