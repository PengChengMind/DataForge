package algorithm.simhash;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class SimhashFactory {

    public static List<Tuple2<Long, Long>> getSimPairs(List<Tuple2<Long, List<Long>>> recordsList, int haming_distance, String debugMode){



        List<Tuple2<Long, Long>> simList = new ArrayList<>();
        if(recordsList.size() < 2){
            return simList;
        }

        // DocHashes is a list that will contain all of the calculated hashes.
        ArrayList<Long> docHashes = Lists.newArrayList();

        // Maps 12-bit key with the documents matching the partial hash
        Map<BitSet, HashSet<Integer>> hashIndex = Maps.newHashMap();

        int groupNum = 64/ (haming_distance + 1);

        Simhash simHash = new Simhash(new JiebaWordSeg());
        int idx = 0;

//        System.out.println("Start to build index...");
        for (Tuple2<Long, List<Long>> doc : recordsList) {
            // Calculate the document hash.
            long docHash = simHash.simhash64(doc._2);
//            System.out.println("Document=[" + doc + "] Hash=[" + docHash + "] length=" + doc._2.size());

            // Store the document hash in a list.
            docHashes.add(docHash);

            // StringBuilder keyBuilder = new StringBuilder(12);
            BitSet key = new BitSet(groupNum);

            int step = 0;
            if(debugMode.equalsIgnoreCase("debug")) {
                System.out.println("binary " + Long.toBinaryString(docHash));
            }
            for (int i = 0; i < 64; i++) {
                key.set(step, ((docHash >> i) & 1) == 1);
//                System.out.println(step);
                if (step++ == (groupNum - 1)) {
                    /*
                     * a) Separates the hash in 12-bit keys. b) This value will
                     * be a key in hashIndex. c) hashIndex will contain sets of
                     * documents matching each key (12-bits).
                     */
                    if (hashIndex.containsKey(key)) {
                        hashIndex.get(key).add(idx);
                    } else {
                        HashSet<Integer> vector = new HashSet<>();
                        vector.add(idx);
                        hashIndex.put(key, vector);
                    }
                    if(debugMode.equalsIgnoreCase("debug")) {
                        System.out.println("bitset " + key + " idx " + idx);
                    }
                    step = 0;
                    key = new BitSet(groupNum); // reset key holder.
                }
            }
            idx++;
        }
        if(debugMode.equalsIgnoreCase("debug")) {
            System.out.println("Index has been built.");
            System.out.println(hashIndex);
        }

        idx = 0;
        BitSet bits = new BitSet(recordsList.size());

        for (Tuple2<Long, List<Long>> doc: recordsList) {
            // For each document.
            if(debugMode.equalsIgnoreCase("debug")) {
                System.out.println("idx current " + idx);
            }
            if (bits.get(idx)) {
                if(debugMode.equalsIgnoreCase("debug")) {
                    System.out.println("idx exist " + idx);
                }
                ++idx;
                continue;
            }

            // Calculates document hash.
            long docHash = simHash.simhash64(doc._2);
            BitSet key = new BitSet(groupNum);

            int step = 0;
            HashSet<Integer> docSimilarCandidates = Sets.newHashSet();
            for (int i = 0; i < 64; ++i) {
                key.set(step, ((docHash >> i) & 1) == 1);

                if (step++ == (groupNum - 1)) {
                    /*
                     * a) Separates the hash in 12-bit keys. b) This value will
                     * be a key in hashIndex. c) hashIndex will contain sets of
                     * documents matching each key (12-bits).
                     */
                    if (hashIndex.containsKey(key)) {
                        docSimilarCandidates.addAll(hashIndex.get(key));
                    }
                    step = 0;
                    key = new BitSet(groupNum);
                }
            }
            if(debugMode.equalsIgnoreCase("debug")) {
                System.out.println("docSimilarCandidates: " + docSimilarCandidates);
            }
            List<Integer> similarDocs = Lists.newLinkedList();
            Map<Integer, Integer> docDistances = Maps.newHashMap();
            for (Integer i : docSimilarCandidates) {
                int dist = simHash.hammingDistance(docHash, docHashes.get(i));
                if(debugMode.equalsIgnoreCase("debug")) {
                    System.out.println("record:" + doc._1 + ":" + recordsList.get(i)._1 + " haming distance " + dist + " docHash " + docHash + " docHashes.get(i) " + docHashes.get(i));
                }
                if (dist <= haming_distance) {
                    similarDocs.add(i);
                    bits.set(i);
                    docDistances.put(i, dist);
                }
            }
            if (!similarDocs.isEmpty()) {
                for (int i : similarDocs) {
                    if (i == idx)
                        continue;
                    simList.add(new Tuple2<>(doc._1, recordsList.get(i)._1));
                    if(debugMode.equalsIgnoreCase("debug")) {
                        System.out.println("Documents similar left [" + doc + "]:\n");
                        System.out.println("Documents similar right [" + recordsList.get(i) + "]\tDistance=[" + docDistances.get(i) + "]\n");
                    }
                    Set<Long> doc_map = new HashSet<>(recordsList.get(i)._2);
                    List<Long> interList = new ArrayList<>();
                    for (Long ele: doc._2){
                        if(doc_map.contains(ele)){
                            interList.add(ele);
                        }
                    }
                    if(debugMode.equalsIgnoreCase("debug")) {
                        System.out.println("inter size " + interList.size() + " similarity " + interList.size() * 1.0 / (doc_map.size() + doc._2.size() - interList.size()));
                        System.out.println("docHash " + Long.toBinaryString(docHash) + " docHashes.get(i) " + Long.toBinaryString(docHashes.get(i)));
                    }
                }
            }
            bits.set(idx);
            ++idx;
        }
        return simList;
    }


    public static List<Tuple2<Long, Long>> getSimWeightPairs(List<Tuple2<Long, Long>> recordsList, int HAMMING_DISTANCE, String matchMode, String debugMode){

        List<Tuple2<Long, Long>> simList = new ArrayList<>();
        if(recordsList.size() < 2){
            return simList;
        }

        // DocHashes is a list that will contain all of the calculated hashes.
        ArrayList<Long> docHashes = Lists.newArrayList();

        // Maps 12-bit key with the documents matching the partial hash
        Map<BitSet, HashSet<Integer>> hashIndex = Maps.newHashMap();

        Simhash simHash = new Simhash(new JiebaWordSeg());


//        System.out.println("Start to build index...");

        int groupNum = 64 / (HAMMING_DISTANCE + 1);
        if(debugMode.equalsIgnoreCase("debug")) {
            System.out.println("groupNum = " + groupNum + " ");
        }

        // 先遍历一次文档集合，构建一个hash冲突表，hash表的key为文档分组(16位4组)后的部分哈希值，value为冲突表，重新编号了idx
        int idx = 0;
        for (Tuple2<Long, Long> doc : recordsList) {
            // Calculate the document hash.
            long docHash = doc._2;
            if(debugMode.equalsIgnoreCase("debug")) {
                System.out.println("prefix ids & hashcodes: " + doc._1 + " , " + doc._2);
                System.out.println("binary " + Long.toBinaryString(docHash) + " origin " + doc._2);
            }
            //docHash存储的是idx对应的simhash值
            docHashes.add(docHash);

            // StringBuilder keyBuilder = new StringBuilder(12);
            BitSet key = new BitSet(groupNum);

            int step = 0;
            int groups = 0;

            for (int i = 0; i < 64; i++) {
                key.set(step, ((docHash >> i) & 1) == 1);
                if (step == (groupNum - 1) && groups < HAMMING_DISTANCE || i == 63) {
                    /*
                     * a) Separates the hash in 12-bit keys. b) This value will
                     * be a key in hashIndex. c) hashIndex will contain sets of
                     * documents matching each key (12-bits).
                     */
                    groups ++;
                    if (hashIndex.containsKey(key)) {
                        hashIndex.get(key).add(idx);
                    } else {
                        HashSet<Integer> idSet = new HashSet<Integer>();
                        idSet.add(idx);
                        hashIndex.put(key, idSet);
                    }
                    if(debugMode.equalsIgnoreCase("debug")) {
                        System.out.println("bitset " + key + " idx " + idx + " step " + step + " i " + i);
                    }
                    step = 0;
                    key = new BitSet(groupNum); // reset key holder.

                } else {
                    step++;
                }

            }
            ++idx;
        }
        //再次遍历文档集合，根据hash冲突表寻找可能与其相似的文档
        idx = 0;
        //bits存储已经和前面遍历过的某个文档判定为相似的记录，避免循环重复，减少相似对数量
        BitSet bits = new BitSet(recordsList.size());
        for (Tuple2<Long, Long> doc: recordsList) {

            if (matchMode.equalsIgnoreCase("fuzzy") && bits.get(idx)) {
                ++idx;
                continue;
            }

            long docHash = doc._2;
            BitSet key = new BitSet(groupNum);

            int step = 0;
            int groups = 0;
            HashSet<Integer> docSimilarCandidates = Sets.newHashSet();
            //把文档拆成4组，分别寻找可能的相似文档
            for (int i = 0; i < 64; ++i) {
                // key:BitSet 存储的是文档的simhash 16位分组
                key.set(step, ((docHash >> i) & 1) == 1);

                if (step == (groupNum - 1) && groups < HAMMING_DISTANCE || i == 63) {
                    groups ++;
                    // 只要文档的simhash有一部分(取决于海明距离,分4组，总的海明距离要<=3，两个文档相似那么4组中至少要有1组相同，如果4组都不同即使只差1位海明距离也会>3)
                    if (hashIndex.containsKey(key)) {
                        docSimilarCandidates.addAll(hashIndex.get(key));
                    }
                    step = 0;
                    key = new BitSet(groupNum);
                } else {
                    step++;
                }
            }
            if(debugMode.equalsIgnoreCase("debug")) {
                System.out.println("docSimilarCandidates: " + docSimilarCandidates);
            }
            List<Integer> similarDocs = Lists.newLinkedList();
            Map<Integer, Integer> docDistances = Maps.newHashMap();
            for (Integer i : docSimilarCandidates) {
                int hammingDistance = simHash.hammingDistance(docHash, docHashes.get(i));
                if(debugMode.equalsIgnoreCase("debug")) {
                    System.out.println("record:" + doc._1 + ":" + recordsList.get(i)._1 + " haming distance " + hammingDistance + " docHash " + docHash + " docHashes.get(i) " + docHashes.get(i));
                }
                if (hammingDistance <= HAMMING_DISTANCE) {
                    similarDocs.add(i);
                    bits.set(i);
                    docDistances.put(i, hammingDistance);
                }
            }
            if (!similarDocs.isEmpty()) {
                for (int i : similarDocs) {
                    if (i == idx) {
                        continue;
                    }

                    /*
                        保证pair的大小顺序便于准确的distinct
                     */
                    long pair_left = doc._1;
                    long pair_right = recordsList.get(i)._1;
                    long temp = 0l;
                    if(pair_left > pair_right){
                        temp = pair_left;
                        pair_left = pair_right;
                        pair_right = temp;
                    }
                    simList.add(new Tuple2<>(pair_left, pair_right));

                    if(debugMode.equalsIgnoreCase("debug")) {
                        System.out.println("Documents weight similar left [" + doc + "]:\n");
                        System.out.println("Documents weight similar right [" + recordsList.get(i) + "]\tDistance=[" + docDistances.get(i) + "]\n");
                        System.out.println("docHash weight " + Long.toBinaryString(docHash) + " docHashes.get(i) " + Long.toBinaryString(docHashes.get(i)));
                    }
                }
            }
            bits.set(idx);
            ++idx;
        }
        return simList;
    }

    public static List<Tuple2<Long, Long>> getSimWeightPairsFromList(List<Tuple2<Long, List<Tuple3<Long, Integer, Double>>>> recordsList, int haming_distance, String matchMode, String debugMode){

        Simhash simHash = new Simhash(new JiebaWordSeg());
        List<Tuple2<Long, Long>> simHashRecordList = new ArrayList<>();
        /*
            scala Tuple2无法用java stream的collect
         */
        for (int i = 0; i < recordsList.size(); i ++){
            Tuple2<Long, List<Tuple3<Long, Integer, Double>>> record = recordsList.get(i);
            long hashCode = simHash.simhash64WithWeight(record._2);
            simHashRecordList.add(new Tuple2<>(record._1, hashCode));
        }
        return getSimWeightPairs(simHashRecordList, haming_distance, matchMode, debugMode);
    }

}
