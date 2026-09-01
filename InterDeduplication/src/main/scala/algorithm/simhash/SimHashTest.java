package algorithm.simhash;

import com.huaban.analysis.jieba.JiebaSegmenter;
import scala.Tuple3;

import java.util.List;

public class SimHashTest {

    public static void main(String[] args) {
        String str1 = "晚风吹起你并肩的白发抚平回忆留下的疤\n" +
                "画中的你低着头说话";
        String str2 = "晚风吹起你并肩的白发抚平回忆留下的疤\n" +
                "画中的你低着说话";
        JiebaSegmenter jieba = new JiebaSegmenter();
        System.out.println(jieba.sentenceProcess(str1));
        System.out.println(jieba.sentenceProcess(str2));
        Simhash simHash = new Simhash(new JiebaWordSeg());
        long code1 = simHash.simhash64(str1);
        long code2 = simHash.simhash64(str2);
        System.out.println("distance: " + simHash.hammingDistance(code1, code2));
        System.out.println(Long.toBinaryString(code1));
        System.out.println(Long.toBinaryString(code2));
    }
}
