import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.apache.spark.{SparkConf, SparkContext}

object testHandlp {
  def main(args: Array[String]): Unit = {

    val str1 = "县府与林dfsa务局今年预dfas计在乌崁、成功水库、铁线、兴仁水库、双湖园区、白沙等地，进行林相更新与新植造林";
    val str2 = "县府与林务局今年预计在乌崁成功水库铁线兴仁水库双湖园区白沙等地进行林相更新与新植造林"

    val result1 = StandardTokenizer.segment(str1)
    val result2 = StandardTokenizer.segment(str2)
    println(result1)
    println(result2)
  }
}