package algorithm.setmerge;

import java.util.*;

class DisjointSetProblem {
  private final int SIZE = 7;
  private int[] father;  
  private static List<Set<String>> resultList = new ArrayList<Set<String>>();

  public static void main(String[] args) {
      String[] str0 = { "aaa", "bbb", "ccc", };
      String[] str1 = { "bbb", "ddd", };
      String[] str2 = { "eee", "fff", };
       String[] str3 = { "ggg", };
       String[] str4 = { "ddd", "hhh", };
       String[] str5 = { "xx", "yy", };
       String[] str6 = { "zz", "yy", };
       String[][] strs = {str0, str1, str2, str3, str4, str5, str6};
       //change String[][] to List<Set>  
       for (String[] str : strs) {
           //when I write--"Arraylist list=Arrays.asList(strArray)","addAll()" is unsupported for such a arraylist.  
           Set<String> set = new HashSet<String>();
           set.addAll(Arrays.asList(str));
           resultList.add(set);
       }

       DisjointSetProblem disjointSet = new DisjointSetProblem();
       disjointSet.disjoin(strs);
      System.out.println("resultList: " + resultList);
   }

   /*
    * 获取hashmap过程
    * */
   public void disjoin(String[][] strings) {
       if (strings == null || strings.length < 2)
           return;
       initial();
       // 获得hash_map：key为字符串，value为一个链表
       Map<String, List<Integer>> map = storeInHashMap(strings);
       // 并查集进行合并
       union(map);
   }

   //in the beginning,each element is in its own "group".  
   public void initial() {
       father = new int[SIZE];
       for (int i = 0; i < SIZE; i++) {
           father[i] = i;
       }
   }

   /* Map<k,v> 
    * key:String 
    * value:List<Integer>-in which sets the string shows up. 
    */
   public Map<String, List<Integer>> storeInHashMap(String[][] strings) {
       Map<String, List<Integer>> map = new HashMap<String, List<Integer>>();
       for (int i = 0; i < SIZE; i++) {
           for (String each : strings[i]) {
               if (!map.containsKey(each)) {
                   List<Integer> list = new ArrayList<Integer>();
                   list.add(i);
                   map.put(each, list);
               } else {
                   map.get(each).add(i);
               }
           }
       }

       // 打印出map
       System.out.println("集合映射所生成的hashmap为：");
       printMap(map);
       return map;
   }

   private void printMap(Map<String, List<Integer>> map) {
       // TODO Auto-generated method stub
       Iterator<Map.Entry<String, List<Integer>>> iter = map.entrySet()
               .iterator();
       while (iter.hasNext()) {
           Map.Entry<String, List<Integer>> entry = iter.next();
           String key = entry.getKey();
           List<Integer> value = entry.getValue();
           System.out.println(key + ":" + value);
       }
       System.out.println();
   }

   /*
    * 对hashmap进行并查集合并操作
    * */
   public void union(Map<String, List<Integer>> map) {
       Iterator<Map.Entry<String, List<Integer>>> it = map.entrySet()
               .iterator();
       while (it.hasNext()) {
           Map.Entry<String, List<Integer>> entry = it.next();
           List<Integer> value = entry.getValue();
           unionHelp(value);//the arrays whose indexes are in the same list should be merged to one set.             
       }
       // 打印出father父节点信息
       System.out.println("hashmap集合合并之后的父节点信息为：");
       printFather(father);//System.out.println("the father array is " + Arrays.toString(father));
       printSetList(resultList);
       //merge two sets  
        for (int i = 0; i < SIZE; i++) {
            if (i != father[i]) {
                // set：无重复元素
                Set<String> dest = resultList.get(father[i]);
                Set<String> source = resultList.get(i);
                dest.addAll(source);
            }
        }
        //clear a set which has been added.  
        // 当B集合添加到A集合后，清空B集合
        for (int i = 0; i < SIZE; i++) {
            if (i != father[i]) {
                resultList.get(i).clear();
            }
        }
        System.out.println("合并后:" + resultList);
    }
 
    public void unionHelp(List<Integer> list) {
        int minFather = getFather(list.get(0));//list[0] is the smaller.  
        // 传过来的list参数已经排好序
        for (int i = 0, size = list.size(); i < size; i++) {
            //father[list.get(i)] = minFather;
            unionHelp(list.get(0),list.get(i));
        }
    }
 
    // 路径压缩
    public int getFather(int x) {
        while (x != father[x]) {
            x = father[x];
        }
        return x;
    }    
 
    private void printFather(int[] fatherNode) {
        // TODO Auto-generated method stub
        for (int node : fatherNode)
            System.out.print(node + " ");
        System.out.println();
    }
 
    private void printSetList(List<Set<String>> list) {
        // TODO Auto-generated method stub
        System.out.print("合并前:");
        for (int i = 0; i < SIZE; i++) {
            System.out.print(list.get(i) + " ");
        }
        System.out.println();
    }
 
    //general union in disjoin set.But we overload it in this case.  
    public void unionHelp(int x, int y) {
        if (father[x] != father[y]) {
            int fx = getFather(x);
            int fy = getFather(y);
            //merge two arrays to the array that has a smaller index.  
            if (fx < fy) {
                father[y] = fx;
            } else {
                father[x] = fy;
            }
        }
    }
    
}
