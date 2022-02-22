package io.github.shanqiang;

import org.junit.Test;

public class ArrayUtilTest {

    @Test
    public void testIntToBytes() {
        assert ArrayUtil.bytesToInt(ArrayUtil.intToBytes(-1)) == -1;
        assert ArrayUtil.bytesToInt(ArrayUtil.intToBytes(0)) == 0;
        assert ArrayUtil.bytesToInt(ArrayUtil.intToBytes(1)) == 1;
        assert ArrayUtil.bytesToInt(ArrayUtil.intToBytes(40000)) == 40000;
    }
}