package tokenizer

import java.io.Serializable

import com.huaban.analysis.jieba.JiebaSegmenter

import scala.collection.JavaConverters._
import org.apache.commons.lang3.StringUtils
import org.utils.Tools


class JiebaTokenizer extends ITokenizer with Serializable {

  val segmenter = new JiebaSegmenter

  override def tokens(doc: String): List[String] = {
    segmenter.sentenceProcess(doc)
      .asScala
      .toList
      .filter(x => Tools.isContainText(x))
  }

  override def tokens(doc: String, stopWords: Set[String]): List[String] = {
    segmenter.sentenceProcess(doc)
      .asScala
      .toList
      .filter(x => !stopWords.contains(x) && Tools.isContainText(x))
  }

}
