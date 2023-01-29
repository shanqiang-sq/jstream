package io.github.shanqiang.offheap.datatype;

import io.github.shanqiang.offheap.InternalUnsafe;
import io.github.shanqiang.offheap.interfazz.Offheap;

public class IntegerOffheap implements Offheap<IntegerOffheap>
{
    private final int value;

    public IntegerOffheap(int value)
    {
        this.value = value;
    }

    @Override
    public long allocAndSerialize(int extraSize) {
        long addr = InternalUnsafe.alloc(extraSize + Integer.BYTES);
        InternalUnsafe.putInt(addr + extraSize, value);
        return addr;
    }

    @Override
    public void free(long addr, int extraSize) {
        InternalUnsafe.free(addr);
    }

    @Override
    public IntegerOffheap deserialize(long addr) {
        if (0L == addr) {
            return null;
        }

        return new IntegerOffheap(InternalUnsafe.getInt(addr));
    }

    @Override
    public int compareTo(long addr) {
        if (0L == addr) {
            throw new NullPointerException();
        }
        int v = InternalUnsafe.getInt(addr);
        return value == v ? 0 : (value < v ? -1 : 1);
    }

    @Override
    public int compareTo(IntegerOffheap o) {
        int v = o.value;
        return value == v ? 0 : (value < v ? -1 : 1);
    }
}
