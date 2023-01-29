package io.github.shanqiang.offheap;

import org.openjdk.jol.info.ClassLayout;

import java.util.concurrent.atomic.AtomicLong;

public class BufferOffheap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BufferOffheap.class).instanceSize();
    private static final AtomicLong bufferSize = new AtomicLong(0L);

    protected final long addr;
    protected final long size;

    static long bufferOffheapSize() {
        return bufferSize.get();
    }

    public BufferOffheap(long size)
    {
        if (size <= 0) {
            throw new IllegalArgumentException("size: " + size);
        }

        this.size = size;
        addr = InternalUnsafe.alloc(size);
        bufferSize.addAndGet(Long.BYTES + size);
    }

    public void init() {
        InternalUnsafe.setMemory(addr, size, (byte) 0);
    }

    public void init0(long offset) {
        InternalUnsafe.setMemory(addr + offset, size - offset, (byte) 0);
    }

    public long getAddr() {
        return addr;
    }

    public long getSize() {
        return size;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (0L == addr) {
            throw new IllegalStateException();
        }
        bufferSize.addAndGet(-(Long.BYTES + size));
        InternalUnsafe.free(addr);
    }
}