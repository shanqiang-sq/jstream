package io.github.shanqiang.offheap.buffer;

import io.github.shanqiang.offheap.BufferOffheap;
import io.github.shanqiang.offheap.InternalUnsafe;
import org.openjdk.jol.info.ClassLayout;

import static io.github.shanqiang.offheap.InternalUnsafe.copyMemory;
import static io.github.shanqiang.offheap.InternalUnsafe.putInt;

public class IntBufferOffheap extends BufferOffheap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IntBufferOffheap.class).instanceSize();

    public IntBufferOffheap(long size)
    {
        super(size * Integer.BYTES);
    }

    public IntBufferOffheap copyFrom(IntBufferOffheap from, long size) {
        InternalUnsafe.copyMemory(from.addr, addr, size);
        return this;
    }

    public IntBufferOffheap copy(long newSize) {
        return new IntBufferOffheap(newSize).copyFrom(this, this.size);
    }

    public void set(long index, int value) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        if ((index + 1) * Integer.BYTES > size) {
            throw new IndexOutOfBoundsException();
        }
        InternalUnsafe.putInt(addr + index * Integer.BYTES, value);
    }

    public int get(long index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        if ((index + 1) * Integer.BYTES > size) {
            throw new IndexOutOfBoundsException();
        }

        return InternalUnsafe.getInt(addr + index * Integer.BYTES);
    }
}