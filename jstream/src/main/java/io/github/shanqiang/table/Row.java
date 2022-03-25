package io.github.shanqiang.table;

import io.github.shanqiang.offheap.ByteArray;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;

public interface Row {
    /**
     * ByteArray to String to avoid unaware use ByteArray as String
     * @param comparable    input value
     * @return              if input value is ByteArray return String else throw exception to remind user
     */
    default String toStr(Comparable comparable) {
        if (null == comparable) {
            return null;
        }
        ByteArray byteArray = (ByteArray) comparable;
        return byteArray.toString();
    }

    default String toStr(Comparable comparable, String charsetName) throws UnsupportedEncodingException {
        if (null == comparable) {
            return null;
        }
        ByteArray byteArray = (ByteArray) comparable;
        return new String(byteArray.getBytes(), byteArray.getOffset(), byteArray.getLength(), charsetName);
    }

    default ByteArray toByteArray(Comparable comparable) {
        if (null == comparable) {
            return null;
        }
        return  (ByteArray) comparable;
    }

    default byte[] toBytes(Comparable comparable) {
        if (null == comparable) {
            return null;
        }
        ByteArray byteArray = (ByteArray) comparable;
        byte[] ret = new byte[byteArray.getLength()];
        System.arraycopy(byteArray.getBytes(), byteArray.getOffset(), ret, 0, byteArray.getLength());
        return ret;
    }

    /**
     *
     * @return LinkedHashMap.LinkedKeySet 保证列的顺序
     */
    Set<String> getColumnNames();
    Comparable[] getAll();
    Comparable getComparable(int index);
    Comparable getComparable(String columnName);
    String getString(String columnName);
    String getString(String columnName, String charsetName) throws UnsupportedEncodingException;
    byte[] getBytes(String columnName);
    ByteArray getByteArray(String columnName);
    BigDecimal getBigDecimal(String columnName);
    Double getDouble(String columnName);
    Long getLong(String columnName);
    Integer getInteger(String columnName);
    String getString(int index);
    String getString(int index, String charsetName) throws UnsupportedEncodingException;
    byte[] getBytes(int index);
    ByteArray getByteArray(int index);
    BigDecimal getBigDecimal(int index);
    Double getDouble(int index);
    Long getLong(int index);
    Integer getInteger(int index);
    int size();
}
