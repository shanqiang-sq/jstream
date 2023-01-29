package io.github.shanqiang.offheap.datatype;

import io.github.shanqiang.offheap.InternalUnsafe;
import io.github.shanqiang.offheap.interfazz.ComparableOffheap;
import io.github.shanqiang.offheap.interfazz.Offheap;

import static io.github.shanqiang.offheap.InternalUnsafe.copyMemory;
import static io.github.shanqiang.offheap.InternalUnsafe.getInt;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class ByteArrayOffheap implements Offheap<ByteArrayOffheap>
{
    private final byte[] value;
    private int hash; // Default to 0

    public ByteArrayOffheap(byte[] value)
    {
        this.value = requireNonNull(value);
    }

    @Override
    public long allocAndSerialize(int extraSize) {
        long addr = InternalUnsafe.alloc(extraSize + Integer.BYTES + value.length * Byte.BYTES);
        InternalUnsafe.putInt(addr + extraSize, value.length);
        InternalUnsafe.copyMemory(value, ARRAY_BYTE_BASE_OFFSET, null, addr + extraSize + Integer.BYTES, value.length);
        return addr;
    }

    @Override
    public void free(long addr, int extraSize) {
        InternalUnsafe.free(addr);
    }

    @Override
    public ByteArrayOffheap deserialize(long addr) {
        if (0L == addr) {
            return null;
        }

        int len = getInt(addr);
        final byte[] bytes = new byte[len];
        copyMemory(null, addr + Integer.BYTES, bytes, ARRAY_BYTE_BASE_OFFSET, len);
        return new ByteArrayOffheap(bytes);
    }

    @Override
    public int compareTo(long addr) {
        if (0L == addr) {
            throw new NullPointerException();
        }
        return ComparableOffheap.compareTo(value, ARRAY_BYTE_BASE_OFFSET, value.length, null, addr + Integer.BYTES, getInt(addr));
    }

    @Override
    public int compareTo(ByteArrayOffheap o) {
        return ComparableOffheap.compareTo(value, ARRAY_BYTE_BASE_OFFSET, value.length, o.value, ARRAY_BYTE_BASE_OFFSET, o.value.length);
    }

    @Override
    public boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }
        if (anObject instanceof ByteArrayOffheap) {
            ByteArrayOffheap that = (ByteArrayOffheap) anObject;
            if (value.length == that.value.length) {
                for (int i = 0; i < value.length; i++) {
                    if (this.value[i] != that.value[i]) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (hash == 0 && value.length > 0) {
            for (int i = 0; i < value.length; i++) {
                hash = 31 * hash + value[i];
            }
        }
        return hash;
    }
}
