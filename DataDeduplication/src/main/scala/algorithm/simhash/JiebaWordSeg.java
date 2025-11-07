/**
 * 
 */
package algorithm.simhash;

import com.huaban.analysis.jieba.JiebaSegmenter;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class JiebaWordSeg implements IWordSeg, Serializable {

	JiebaSegmenter segmenter;

	public JiebaWordSeg(){
		segmenter = new JiebaSegmenter();
	}

	@Override
	public List<String> tokens(String doc) {

		List<String> binaryWords = segmenter.sentenceProcess(doc);
		return binaryWords;
	}

	@Override
	public List<String> tokens(String doc, Set<String> stopWords) {
		return null;
	}

}
