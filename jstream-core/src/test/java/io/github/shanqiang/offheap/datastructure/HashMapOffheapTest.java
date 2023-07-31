//package io.github.shanqiang.offheap.datastructure;
//
//import io.github.shanqiang.offheap.InternalUnsafe;
//import io.github.shanqiang.offheap.datatype.ByteArrayOffheap;
//import org.junit.Test;
//
//import java.util.*;
//
//public class HashMapOffheapTest {
//    @Test
//    public void test() {
//        long pre = InternalUnsafe.getUsedMemory();
//        HashMapOffheap<ByteArrayOffheap, ByteArrayOffheap> hashMapOffheap = new HashMapOffheap<>();
//        ByteArrayOffheap key = new ByteArrayOffheap("aaa".getBytes());
//        ByteArrayOffheap value = new ByteArrayOffheap("bbb".getBytes());
//        hashMapOffheap.put(key, value);
//        assert hashMapOffheap.get(key).compareTo(value) == 0;
//        assert hashMapOffheap.get(key).equals(value);
//        // 批量跑test的情况下前面的test里创建的内存pre可能在这时被GC回收了，因此是小等于234而不是直等于234
//        assert InternalUnsafe.getUsedMemory() - pre <= 234;
//        ByteArrayOffheap ccc = new ByteArrayOffheap("ccc".getBytes());
//        hashMapOffheap.put(key, ccc);
//        assert hashMapOffheap.get(key).equals(ccc);
//
//        hashMapOffheap.free();
//        assert InternalUnsafe.getUsedMemory() <= pre;
//
//        Set<ByteArrayOffheap> set = new HashSet<>();
//        hashMapOffheap = new HashMapOffheap<>();
//        for (int i = 0; i < 17; i++) {
//            ByteArrayOffheap k = new ByteArrayOffheap(("k" + i).getBytes());
//            ByteArrayOffheap v = new ByteArrayOffheap(("v" + i).getBytes());
//            set.add(v);
//            hashMapOffheap.put(k, v);
//        }
//        Set<ByteArrayOffheap> set1 = new HashSet<>();
//        Iterator<ByteArrayOffheap> it = hashMapOffheap.iterator();
//        while (it.hasNext()) {
//            key = it.next();
//            value = hashMapOffheap.get(key);
//            set1.add(value);
//        }
//        assert set.equals(set1);
//
//        hashMapOffheap.free();
//        assert InternalUnsafe.getUsedMemory() <= pre;
//
//        int total = 200_0000;
//        Map<String, String> map = new HashMap<>();
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < total; i++) {
//            String k = "k" + i;
//            String v = "v" + i;
//            map.put(k, v);
//        }
//        long end = System.currentTimeMillis();
//        System.out.println("HashMap put qps: " + total/((end - start)/1000.0));
//        System.out.println("HashMap put elapse: " + ((end - start)/1000.0));
//
//        start = System.currentTimeMillis();
//        for (int i = 0; i < total; i++) {
//            String k = "k" + i;
//            String v = "v" + i;
//            assert v.equals(map.get(k));
//        }
//        end = System.currentTimeMillis();
//        System.out.println("HashMap get qps: " + total/((end - start)/1000.0));
//        System.out.println("HashMap get elapse: " + ((end - start)/1000.0));
//
//        hashMapOffheap = new HashMapOffheap<>();
//        long startOffheap = System.currentTimeMillis();
//        for (int i = 0; i < total; i++) {
//            ByteArrayOffheap k = new ByteArrayOffheap(("k" + i).getBytes());
//            ByteArrayOffheap v = new ByteArrayOffheap(("v" + i).getBytes());
//            hashMapOffheap.put(k, v);
//        }
//        long endOffheap = System.currentTimeMillis();
//        System.out.println("put qps: " + total/((endOffheap - startOffheap)/1000.0));
//        System.out.println("put elapse: " + ((endOffheap - startOffheap)/1000.0));
//
//        startOffheap = System.currentTimeMillis();
//        for (int i = 0; i < total; i++) {
//            ByteArrayOffheap k = new ByteArrayOffheap(("k" + i).getBytes());
//            ByteArrayOffheap v = hashMapOffheap.get(k);
//            assert v.equals(new ByteArrayOffheap(("v" + i).getBytes()));
//        }
//        endOffheap = System.currentTimeMillis();
//        System.out.println("get qps: " + total/((endOffheap - startOffheap)/1000.0));
//        System.out.println("get elapse: " + ((endOffheap - startOffheap)/1000.0));
//
//        assert hashMapOffheap.size() == total;
//
//        startOffheap = System.currentTimeMillis();
//        for (int i = 0; i < total; i++) {
//            ByteArrayOffheap k = new ByteArrayOffheap(("k" + i).getBytes());
//            ByteArrayOffheap v = new ByteArrayOffheap(("vvv" + i).getBytes());
//            hashMapOffheap.put(k, v);
//        }
//        endOffheap = System.currentTimeMillis();
//        System.out.println("put2 qps: " + total/((endOffheap - startOffheap)/1000.0));
//        System.out.println("put2 elapse: " + ((endOffheap - startOffheap)/1000.0));
//
//        assert hashMapOffheap.size() == total;
//
//        startOffheap = System.currentTimeMillis();
//        for (int i = 0; i < total; i++) {
//            ByteArrayOffheap k = new ByteArrayOffheap(("k" + i).getBytes());
//            ByteArrayOffheap v = hashMapOffheap.get(k);
//            assert v.equals(new ByteArrayOffheap(("vvv" + i).getBytes()));
//        }
//        endOffheap = System.currentTimeMillis();
//        System.out.println("get2 qps: " + total/((endOffheap - startOffheap)/1000.0));
//        System.out.println("get2 elapse: " + ((endOffheap - startOffheap)/1000.0));
//
//        assert hashMapOffheap.size() == total;
//
//        startOffheap = System.currentTimeMillis();
//        for (int i = 0; i < total; i++) {
//            ByteArrayOffheap k = new ByteArrayOffheap(("k" + i).getBytes());
//            assert  hashMapOffheap.remove(k);
//        }
//        endOffheap = System.currentTimeMillis();
//        System.out.println("remove qps: " + total/((endOffheap - startOffheap)/1000.0));
//        System.out.println("remove elapse: " + ((endOffheap - startOffheap)/1000.0));
//
//        assert hashMapOffheap.size() == 0;
//
//        hashMapOffheap.free();
//        assert InternalUnsafe.getUsedMemory() <= pre;
//        System.out.println("free elapse: " + ((System.currentTimeMillis() - endOffheap)/1000.0));
//    }
//}