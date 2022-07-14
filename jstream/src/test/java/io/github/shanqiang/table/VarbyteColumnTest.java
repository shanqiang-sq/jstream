package io.github.shanqiang.table;

import org.junit.Test;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class VarbyteColumnTest {
    @Test
    public void test() {
        VarbyteColumn varbyteColumn = new VarbyteColumn(2);
        varbyteColumn.add("");
        varbyteColumn.add("");
        varbyteColumn.add(null);
        varbyteColumn.add(null);
        long len = varbyteColumn.serializeSize();
        byte[] bytes = new byte[(int) len];
        varbyteColumn.serialize(bytes, 0, len);

        VarbyteColumn varbyteColumn1 = new VarbyteColumn();
        varbyteColumn1.deserialize(bytes, 0 + ARRAY_BYTE_BASE_OFFSET, len);
        assert varbyteColumn1.get(0).toString().equals("");
        assert varbyteColumn1.get(1).toString().equals("");
        assert varbyteColumn1.get(2) == null;
        assert varbyteColumn1.get(3) == null;
    }
}