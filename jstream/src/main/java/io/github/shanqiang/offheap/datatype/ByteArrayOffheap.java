package io.github.shanqiang.offheap.datatype;

import io.github.shanqiang.offheap.ByteArray;
import io.github.shanqiang.offheap.InternalUnsafe;
import io.github.shanqiang.offheap.interfazz.HashCoder;
import io.github.shanqiang.offheap.interfazz.Offheap;

import java.util.Objects;

import static io.github.shanqiang.offheap.InternalUnsafe.copyMemory;
import static io.github.shanqiang.offheap.InternalUnsafe.getByte;
import static io.github.shanqiang.offheap.InternalUnsafe.getInt;
import static io.github.shanqiang.offheap.InternalUnsafe.getLong;
import static java.lang.Math.min;
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
        return compareTo(value, ARRAY_BYTE_BASE_OFFSET, value.length, null, addr + Integer.BYTES, getInt(addr));
    }

    @Override
    public int compareTo(ByteArrayOffheap o) {
        return compareTo(value, ARRAY_BYTE_BASE_OFFSET, value.length, o.value, ARRAY_BYTE_BASE_OFFSET, o.value.length);
    }

    private int compareTo(Object thisObj, long thisAddr, int thisSize, Object thatObj, long thatAddr, int thatSize)
    {
        int compareLength;
        for (compareLength = min(thisSize, thatSize); compareLength >= 8; compareLength -= 8) {
            long thisLong = getLong(thisObj, thisAddr);
            long thatLong = getLong(thatObj, thatAddr);
            if (thisLong != thatLong) {
                return longBytesToLong(thisLong) < longBytesToLong(thatLong) ? -1 : 1;
            }

            thisAddr += 8L;
            thatAddr += 8L;
        }

        while (compareLength > 0) {
            byte thisByte = getByte(thisObj, thisAddr);
            byte thatByte = getByte(thatObj, thatAddr);
            int v = (thisByte & 255) - (thatByte & 255);
            if (v != 0) {
                return v;
            }

            thisAddr++;
            thatAddr++;
            compareLength--;
        }

        return Integer.compare(thisSize, thatSize);
    }

    private static long longBytesToLong(long bytes) {
        return Long.reverseBytes(bytes) ^ 0x8000000000000000L;
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
        int h = hash;
        int length = value.length;
        if (h == 0 && length > 0) {
            int mod = length % Long.BYTES;
            for (int i = 0; i < length - mod; i += Long.BYTES) {
                long l = getLong(value, ARRAY_BYTE_BASE_OFFSET + i);
                h = 31 * h + (int) (l ^ l >>> 32);
            }

            for (int i = length - mod; i < length; i++) {
                h = 31 * h + value[i];
            }
            hash = h;
        }
        return h;
    }
}
