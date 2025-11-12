package tokenizer

trait ITokenizer {


  def tokens(doc: String): List[String]

  def tokens(doc: String, stopWords: Set[String]): List[String]

}
