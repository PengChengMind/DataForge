/**
 * 
 */
package algorithm.simhash;

import java.util.List;
import java.util.Set;

public interface IWordSeg {

	public List<String> tokens(String doc);
	
	public List<String> tokens(String doc, Set<String> stopWords);
}
