package io.github.shanqiang.offheap.buffer;

import io.github.shanqiang.offheap.BufferOffheap;
import io.github.shanqiang.offheap.InternalUnsafe;
import org.openjdk.jol.info.ClassLayout;

import static io.github.shanqiang.offheap.InternalUnsafe.copyMemory;
import static io.github.shanqiang.offheap.InternalUnsafe.putLong;

public class LongBufferOffheap extends BufferOffheap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongBufferOffheap.class).instanceSize();

    public LongBufferOffheap(long size)
    {
        super(size * Long.BYTES);
    }

    private LongBufferOffheap copyFrom(LongBufferOffheap from, long size) {
        InternalUnsafe.copyMemory(from.addr, addr, size);
        return this;
    }

    public LongBufferOffheap copy(long newSize) {
        return new LongBufferOffheap(newSize).copyFrom(this, this.size);
    }

    public void set(long index, long value) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        if ((index + 1) * Long.BYTES > size) {
            throw new IndexOutOfBoundsException();
        }
        InternalUnsafe.putLong(addr + index * Long.BYTES, value);
    }

    public long get(long index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        if ((index + 1) * Long.BYTES > size) {
            throw new IndexOutOfBoundsException();
        }

        return InternalUnsafe.getLong(addr + index * Long.BYTES);
    }
}