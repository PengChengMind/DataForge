package algorithm.setmerge;

import java.util.*;

public class SetMergeLong {
    public int SIZE;
    private int[] father;
    private boolean debugMode;
    private List<Set<Long>> resultList = new ArrayList<>();

    public SetMergeLong(boolean debugMode) {
        this.debugMode = debugMode;
    }

    public static void main(String[] args) {
      Long[] str0 = {1l ,2l};
      Long[] str1 = {3l, 4l};
      Long[] str2 = {1l, 4l};
       Long[][] set_arrays = {str0, str1, str2};
       //change String[][] to List<Set>
       SetMergeLong disjointSet = new SetMergeLong(true);
       disjointSet.disjoin(set_arrays);
   }


    /*
    * 获取hashmap过程
    * */
   public List<Set<Long>> disjoin(Long[][] sets) {
       this.SIZE = sets.length;
//       System.out.println("SIZE = " + SIZE);
//       for (int i = 0; i < sets.length; i++){
//           System.out.println("size " + i + " = " + sets[i].length);
//       }
       for (Long[] set_array : sets) {
           //when I write--"Arraylist list=Arrays.asList(strArray)","addAll()" is unsupported for such a arraylist.
           Set<Long> set = new HashSet<>();
           set.addAll(Arrays.asList(set_array));
           resultList.add(set);
       }
       if (sets == null || SIZE < 2)
           return resultList;

       initial();
       // 获得hash_map：key为字符串，value为一个链表
       Map<Long, List<Integer>> map = storeInHashMap(sets);
       // 并查集进行合并
       union(map);
       return resultList;
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
   public Map<Long, List<Integer>> storeInHashMap(Long[][] sets) {
       Map<Long, List<Integer>> map = new HashMap<>();
       for (int i = 0; i < SIZE; i++) {
           for (Long id : sets[i]) {
               if (!map.containsKey(id)) {
                   List<Integer> list = new ArrayList<>();
                   list.add(i);
                   map.put(id, list);
               } else {
                   map.get(id).add(i);
               }
           }
       }

       // 打印出map
       if(debugMode) {
           System.out.println("集合映射所生成的hashmap为：");
           printMap(map);
       }
       return map;
   }

   private void printMap(Map<Long, List<Integer>> map) {
       // TODO Auto-generated method stub
       Iterator<Map.Entry<Long, List<Integer>>> iter = map.entrySet()
               .iterator();
       while (iter.hasNext()) {
           Map.Entry<Long, List<Integer>> entry = iter.next();
           Long key = entry.getKey();
           List<Integer> value = entry.getValue();
           System.out.println(key + ":" + value);
       }
       System.out.println();
   }

   /*
    * 对hashmap进行并查集合并操作
    * */
   public void union(Map<Long, List<Integer>> map) {
       Iterator<Map.Entry<Long, List<Integer>>> it = map.entrySet()
               .iterator();
       while (it.hasNext()) {
           Map.Entry<Long, List<Integer>> entry = it.next();
           List<Integer> value = entry.getValue();
           unionHelp(value);//the arrays whose indexes are in the same list should be merged to one set.             
       }
       if(debugMode) {
           // 打印出father父节点信息
           System.out.println("hashmap集合合并之后的父节点信息为：");
           printFather(father);//System.out.println("the father array is " + Arrays.toString(father));
       }
//       printSetList(resultList);
       //merge two sets  
        for (int i = 0; i < SIZE; i++) {
            if (i != father[i]) {
                // set：无重复元素
                Set<Long> dest = resultList.get(father[i]);
                Set<Long> source = resultList.get(i);
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
        if(debugMode) {
            System.out.println("合并后:" + resultList);
        }
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
 
    private void printSetList(List<Set<Long>> list) {
        // TODO Auto-generated method stub
        System.out.print("合并前: size=" + SIZE);
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
