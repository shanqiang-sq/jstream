package io.github.shanqiang.offheap.datastructure;

import io.github.shanqiang.offheap.InternalUnsafe;
import io.github.shanqiang.offheap.datatype.ByteArrayOffheap;
import org.junit.Test;

public class HashMapOffheapTest {
    @Test
    public void test() {
        HashMapOffheap<ByteArrayOffheap, ByteArrayOffheap> hashMapOffheap = new HashMapOffheap<>();
        ByteArrayOffheap key = new ByteArrayOffheap("aaa".getBytes());
        ByteArrayOffheap value = new ByteArrayOffheap("bbb".getBytes());
        hashMapOffheap.put(key, value);
        assert hashMapOffheap.get(key).compareTo(value) == 0;
        assert hashMapOffheap.get(key).equals(value);
        assert InternalUnsafe.getUsedMemory() == 234;
        hashMapOffheap.free();
        assert InternalUnsafe.getUsedMemory() == 0;
        hashMapOffheap = new HashMapOffheap<>();
        int total = 200_0000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            ByteArrayOffheap k = new ByteArrayOffheap(("k" + i).getBytes());
            ByteArrayOffheap v = new ByteArrayOffheap(("v" + i).getBytes());
            hashMapOffheap.put(k, v);
        }
        long end = System.currentTimeMillis();
        System.out.println("put qps: " + total/((end - start)/1000.0));
        System.out.println("put elapse: " + ((end - start)/1000.0));

        start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            ByteArrayOffheap k = new ByteArrayOffheap(("k" + i).getBytes());
            ByteArrayOffheap v = hashMapOffheap.get(k);
            assert v.equals(new ByteArrayOffheap(("v" + i).getBytes()));
        }
        end = System.currentTimeMillis();
        System.out.println("get qps: " + total/((end - start)/1000.0));
        System.out.println("get elapse: " + ((end - start)/1000.0));
        hashMapOffheap.free();
        assert InternalUnsafe.getUsedMemory() == 0;
    }
}