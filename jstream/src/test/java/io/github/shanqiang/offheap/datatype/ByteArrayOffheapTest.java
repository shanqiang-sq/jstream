package io.github.shanqiang.offheap.datatype;

import io.github.shanqiang.offheap.InternalUnsafe;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class ByteArrayOffheapTest {
    @Test
    public void test() {
        byte[] bytes = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        ByteArrayOffheap byteArrayOffheap = new ByteArrayOffheap(bytes);
        long addr = byteArrayOffheap.allocAndSerialize(0);
        assert InternalUnsafe.getUsedMemory() == Long.BYTES + Integer.BYTES + bytes.length;
        assert byteArrayOffheap.compareTo(addr) == 0;
        ByteArrayOffheap byteArrayOffheap1 = new ByteArrayOffheap("bbcdefghij".getBytes(StandardCharsets.UTF_8));
        assert byteArrayOffheap.deserialize(addr).compareTo(byteArrayOffheap1) < 0;
        assert byteArrayOffheap1.compareTo(addr) > 0;
        assert byteArrayOffheap1.compareTo(byteArrayOffheap) > 0;
        byteArrayOffheap.free(addr, 0);
        assert InternalUnsafe.getUsedMemory() == 0;
    }
}