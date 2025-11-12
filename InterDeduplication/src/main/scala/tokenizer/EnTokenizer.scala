package tokenizer

import java.io.Serializable

import com.huaban.analysis.jieba.JiebaSegmenter

import scala.collection.JavaConverters._


class EnTokenizer extends ITokenizer with Serializable {

  val segmenter = new JiebaSegmenter

  override def tokens(doc: String): List[String] = {
    doc.split("[^a-zA-Z]+")
      .toList
  }

  override def tokens(doc: String, stopWords: Set[String]): List[String] = {
    doc.split("[^a-zA-Z]+")
      .toList
      .filter(!stopWords.contains(_))
  }
}
