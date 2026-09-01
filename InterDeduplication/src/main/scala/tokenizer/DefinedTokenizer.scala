package tokenizer

import java.io.Serializable

import com.huaban.analysis.jieba.JiebaSegmenter


class DefinedTokenizer(separator: String) extends ITokenizer with Serializable {

  def tokens(doc: String): List[String] = {
    doc.split(separator)
      .toList
  }

  override def tokens(doc: String, stopWords: Set[String]): List[String] = {
    doc.split(separator)
      .toList
      .filter(!stopWords.contains(_))
  }
}
