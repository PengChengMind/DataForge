package tokenizer

import com.huaban.analysis.jieba.JiebaSegmenter

import java.io.Serializable


class SepTokenizer extends ITokenizer with Serializable {

  val segmenter = new JiebaSegmenter

  override def tokens(doc: String): List[String] = {
    doc.split("[\\pP\\pS\\s]+")
      .toList
  }

  override def tokens(doc: String, stopWords: Set[String]): List[String] = {
    println("tokens input doc:" + doc)
    val result = doc.split("[\\pP\\pS\\s]+")
      .toList
      .filter(!stopWords.contains(_))
    println("tokens output doc:" + result)
    return result
  }
}
