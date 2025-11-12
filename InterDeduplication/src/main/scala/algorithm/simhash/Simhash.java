/**
 * 
 */
package algorithm.simhash;

import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.List;

public class Simhash implements Serializable {

	private IWordSeg wordSeg;
	private int hammingDistance;

	public Simhash(IWordSeg wordSeg) {
		this.wordSeg = wordSeg;
	}

	public Simhash() { }

	public int hammingDistance(int hash1, int hash2) {
		int i = hash1 ^ hash2;
		i = i - ((i >>> 1) & 0x55555555);
		i = (i & 0x33333333) + ((i >>> 2) & 0x33333333);
		i = (i + (i >>> 4)) & 0x0f0f0f0f;
		i = i + (i >>> 8);
		i = i + (i >>> 16);
		return i & 0x3f;
	}

	public int hammingDistance(long hash1, long hash2) {
		long i = hash1 ^ hash2;
//		System.out.println(Long.toBinaryString(hash1) + ":" + Long.toBinaryString(hash2) + " haming_distance " + i + " : " + Long.toBinaryString(i));
		i = i - ((i >>> 1) & 0x5555555555555555L);
		i = (i & 0x3333333333333333L) + ((i >>> 2) & 0x3333333333333333L);
		i = (i + (i >>> 4)) & 0x0f0f0f0f0f0f0f0fL;
		i = i + (i >>> 8);
		i = i + (i >>> 16);
		i = i + (i >>> 32);
		return (int) i & 0x7f;
	}

	public long simhash64(String doc) {
		int bitLen = 64;
		int[] bits = new int[bitLen];
		List<String> tokens = wordSeg.tokens(doc);
		for (String t : tokens) {
			long v = MurmurHash.hash64(t);
			for (int i = bitLen; i >= 1; --i) {
				if (((v >> (bitLen - i)) & 1) == 1)
					++bits[i - 1];
				else
					--bits[i - 1];
			}
		}
		long hash = 0x0000000000000000;
		long one = 0x0000000000000001;
		for (int i = bitLen; i >= 1; --i) {
			if (bits[i - 1] > 0) {
				hash |= one;
			}
			one = one << 1;
		}
		return hash;
	}

	public long simhash64StringWithWeight(List<Tuple2<String,Double>> doc) {
		int bitLen = 64;
		int[] bits = new int[bitLen];
		for (Tuple2<String, Double> t : doc) {
			long v = MurmurHash.hash64(t._1);
			for (int i = bitLen; i >= 1; --i) {
				if (((v >> (bitLen - i)) & 1) == 1)
					bits[i - 1] += t._2;
				else
					bits[i - 1] -= t._2;
			}
		}
		long hash = 0x0000000000000000;
		long one = 0x0000000000000001;
		for (int i = bitLen; i >= 1; --i) {
			if (bits[i - 1] > 0) {
				hash |= one;
			}
			one = one << 1;
		}
		return hash;
	}

	public long simhash64(List<Long> tokens) {
		int bitLen = 64;
		int[] bits = new int[bitLen];
		for (Long v : tokens) {
			for (int i = bitLen; i >= 1; --i) {
				if (((v >> (bitLen - i)) & 1) == 1)
					bits[i - 1] += v;
				else
					bits[i - 1] -= v;
			}
		}
		long hash = 0x0000000000000000;
		long one = 0x0000000000000001;
		for (int i = bitLen; i >= 1; --i) {
			if (bits[i - 1] > 0) {
				hash |= one;
			}
			one = one << 1;
		}
		return hash;
	}

	public long simhash64WithWeight(List<Tuple3<Long, Integer, Double>> tokens) {
		int bitLen = 64;
		double[] bits = new double[bitLen];
		for (Tuple3<Long, Integer, Double> v : tokens) {
			for (int i = bitLen; i >= 1; --i) {
				if (((v._1() >> (bitLen - i)) & 1) == 1)
					bits[i - 1] += v._3();
				else
					bits[i - 1] -= v._3();
			}
		}
		long hash = 0x0000000000000000;
		long one = 0x0000000000000001;
		for (int i = bitLen; i >= 1; --i) {
			if (bits[i - 1] > 0) {
				hash |= one;
			}
			one = one << 1;
		}
		return hash;
	}

	public int simhash32(String doc) {
		int bitLen = 32;
		int[] bits = new int[bitLen];
		List<String> tokens = wordSeg.tokens(doc);
		for (String t : tokens) {
			int v = MurmurHash.hash32(t);
			for (int i = bitLen; i >= 1; --i) {
				if (((v >> (bitLen - i)) & 1) == 1)
					++bits[i - 1];
				else
					--bits[i - 1];
			}
		}
		int hash = 0x00000000;
		int one = 0x00000001;
		for (int i = bitLen; i >= 1; --i) {
			if (bits[i - 1] > 1) {
				hash |= one;
			}
			one = one << 1;
		}
		return hash;
	}

	public int simhash32(List<Integer> tokens) {
		int bitLen = 32;
		int[] bits = new int[bitLen];
		for (Integer v : tokens) {
			for (int i = bitLen; i >= 1; --i) {
				if (((v >> (bitLen - i)) & 1) == 1)
					++bits[i - 1];
				else
					--bits[i - 1];
			}
		}
		int hash = 0x00000000;
		int one = 0x00000001;
		for (int i = bitLen; i >= 1; --i) {
			if (bits[i - 1] > 1) {
				hash |= one;
			}
			one = one << 1;
		}
		return hash;
	}

	public long simhash32WithWeight(List<Tuple2<Integer, Double>> tokens) {
		int bitLen = 32;
		double[] bits = new double[bitLen];
		for (Tuple2<Integer, Double> v : tokens) {
			for (int i = bitLen; i >= 1; --i) {
				if (((v._1 >> (bitLen - i)) & 1) == 1)
					bits[i - 1] += v._2;
				else
					bits[i - 1] -= v._2;
			}
		}
		long hash = 0x00000000;
		long one = 0x00000001;
		for (int i = bitLen; i >= 1; --i) {
			if (bits[i - 1] > 0) {
				hash |= one;
			}
			one = one << 1;
		}
		return hash;
	}
}
