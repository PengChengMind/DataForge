/**
 * 
 */
package algorithm.simhash;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author zhangcheng
 * 
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {


		String inputPath = "/Users/stardust/StardustScalaProjects/SparkScalaProjects/input/mini.txt";
		String outputPath = "/Users/stardust/data/simhash-java/cnews-3.4g/output-3001.txt";
		final int haming_distance = 5;

		if (args.length == 2) {
			inputPath = args[0];
			outputPath = args[1];
		}
		long start = System.nanoTime();
		// Creates SimHash object.
		Simhash simHash = new Simhash(new JiebaWordSeg());

		// DocHashes is a list that will contain all of the calculated hashes.
		ArrayList<Long> docHashes = Lists.newArrayList();

		// Maps 12-bit key with the documents matching the partial hash
		Map<BitSet, HashSet<Integer>> hashIndex = Maps.newHashMap();

		// Read the documents. (Each line represents a document).
		List<String> docs = readDocs(inputPath);
		System.out.println("doc count = " + docs.size());

		int idx = 0;

		System.out.println("Start to build index...");
		for (String doc : docs) {
			// Calculate the document hash.
			long docHash = simHash.simhash64(doc);
			System.out.println("Document=[" + doc + "] Hash=[" + docHash + "]");

			// Store the document hash in a list.
			docHashes.add(docHash);

			// StringBuilder keyBuilder = new StringBuilder(12);
			BitSet key = new BitSet(12);

			int step = 0;

			for (int i = 0; i < 64; ++i) {
				key.set(step, ((docHash >> i) & 1) == 1);
				if (step++ == 12) {
					/*
					 * a) Separates the hash in 12-bit keys. b) This value will
					 * be a key in hashIndex. c) hashIndex will contain sets of
					 * documents matching each key (12-bits).
					 */
					if (hashIndex.containsKey(key)) {
						hashIndex.get(key).add(idx);
					} else {
						HashSet<Integer> vector = new HashSet<Integer>();
						vector.add(idx);
						hashIndex.put(key, vector);
					}
					step = 0;
					key = new BitSet(12); // reset key holder.
				}
			}
			++idx;
		}
		System.out.println("Index has been built.");
		File output = new File(outputPath);
		idx = 0;
		BitSet bits = new BitSet(docs.size());

		for (String doc : docs) {
			// For each document.

			if (bits.get(idx)) {
				++idx;
				continue;
			}

			// Calculates document hash.
			long docHash = simHash.simhash64(doc);
			BitSet key = new BitSet(12);

			int step = 0;
			HashSet<Integer> docSimilarCandidates = Sets.newHashSet();
			for (int i = 0; i < 64; ++i) {
				key.set(step, ((docHash >> i) & 1) == 1);

				if (step++ == 12) {
					/*
					 * a) Separates the hash in 12-bit keys. b) This value will
					 * be a key in hashIndex. c) hashIndex will contain sets of
					 * documents matching each key (12-bits).
					 */
					if (hashIndex.containsKey(key)) {
						docSimilarCandidates.addAll(hashIndex.get(key));
					}
					step = 0;
					key = new BitSet(12);
				}
			}
			List<Integer> similarDocs = Lists.newLinkedList();
			Map<Integer, Integer> docDistances = Maps.newHashMap();
			for (Integer i : docSimilarCandidates) {
				int dist = simHash.hammingDistance(docHash, docHashes.get(i));
				if (dist <= haming_distance) {
					similarDocs.add(i);
					bits.set(i);
					docDistances.put(i, dist);
				}
			}
			if (!similarDocs.isEmpty()) {
				Files.append("Documents similar as [" + doc + "]:\n", output, Charsets.UTF_8);
				System.out.println("Documents similar as [" + doc + "]:\n");
				for (int i : similarDocs) {
					if (i == idx)
						continue;
					System.out.println("[" + docs.get(i) + "]\tDistance=[" + docDistances.get(i) + "]\n");
					Files.append("[" + docs.get(i) + "]\tDistance=[" + docDistances.get(i) + "]\n", output, Charsets.UTF_8);
				}
				Files.append("End\n", output, Charsets.UTF_8);
			}
			bits.set(idx);
			++idx;
		}

		System.out.println("Elapsed time: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
	}

	private static List<String> readDocs(String inputPath) throws IOException {
		return Files.readLines(new File(inputPath), Charsets.UTF_8).stream().filter(x -> x.length() > 5).collect(Collectors.toList());
	}
}
