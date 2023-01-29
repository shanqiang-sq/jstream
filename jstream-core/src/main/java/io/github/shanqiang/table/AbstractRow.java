package io.github.shanqiang.table;

import io.github.shanqiang.offheap.ByteArray;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

import static io.github.shanqiang.util.ScalarUtil.*;

public abstract class AbstractRow implements Row {
    @Override
    public Comparable[] getAll() {
        int len = size();
        Comparable[] comparables = new Comparable[len];
        for (int i = 0; i < len; i++) {
            comparables[i] = getComparable(i);
        }

        return comparables;
    }

    @Override
    public String getString(String columnName) {
        return toStr(getComparable(columnName));
    }

    @Override
    public String getString(String columnName, String charsetName) throws UnsupportedEncodingException {
        return toStr(getComparable(columnName), charsetName);
    }

    @Override
    public byte[] getBytes(String columnName) {
        return toBytes(getComparable(columnName));
    }

    @Override
    public ByteArray getByteArray(String columnName) {
        return toByteArray(getComparable(columnName));
    }

    @Override
    public BigDecimal getBigDecimal(String columnName) {
        return toBigDecimal(getComparable(columnName));
    }

    @Override
    public Double getDouble(String columnName) {
        return toDouble(getComparable(columnName));
    }

    @Override
    public Long getLong(String columnName) {
        return toLong(getComparable(columnName));
    }

    @Override
    public Integer getInteger(String columnName) {
        return toInteger(getComparable(columnName));
    }

    @Override
    public String getString(int index) {
        return toStr(getComparable(index));
    }

    @Override
    public String getString(int index, String charsetName) throws UnsupportedEncodingException {
        return toStr(getComparable(index), charsetName);
    }

    @Override
    public byte[] getBytes(int index) {
        return toBytes(getComparable(index));
    }

    @Override
    public ByteArray getByteArray(int index) {
        return toByteArray(getComparable(index));
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return toBigDecimal(getComparable(index));
    }

    @Override
    public Double getDouble(int index) {
        return toDouble(getComparable(index));
    }

    @Override
    public Long getLong(int index) {
        return toLong(getComparable(index));
    }

    @Override
    public Integer getInteger(int index) {
        return toInteger(getComparable(index));
    }
}
