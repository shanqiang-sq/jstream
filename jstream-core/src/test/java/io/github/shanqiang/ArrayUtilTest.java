package io.github.shanqiang;

import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ArrayUtilTest {
    private volatile int k;

    @Test
    public void testIntToBytes() {
        assert ArrayUtil.bytesToInt(ArrayUtil.intToBytes(-1)) == -1;
        assert ArrayUtil.bytesToInt(ArrayUtil.intToBytes(0)) == 0;
        assert ArrayUtil.bytesToInt(ArrayUtil.intToBytes(1)) == 1;
        assert ArrayUtil.bytesToInt(ArrayUtil.intToBytes(40000)) == 40000;
    }

    @Test
    public void testList() {
        int total = 5000_0000;
        long start = System.currentTimeMillis();
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            list.add(i);
        }
        long end = System.currentTimeMillis();
        System.out.println("elapse: " + (end - start));

        start = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            k = list.get(i);
        }
        end = System.currentTimeMillis();
        System.out.println("elapse: " + (end - start));

        start = System.currentTimeMillis();
        list = new LinkedList<>();
        for (int i = 0; i < total; i++) {
            list.add(i);
        }
        end = System.currentTimeMillis();
        System.out.println("elapse: " + (end - start));

        start = System.currentTimeMillis();
        for (Integer i : list) {
            k = i;
        }
        end = System.currentTimeMillis();
        System.out.println("elapse: " + (end - start));
    }
}