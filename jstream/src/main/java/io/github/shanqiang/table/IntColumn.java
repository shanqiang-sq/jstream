package io.github.shanqiang.table;

import io.github.shanqiang.ArrayUtil;
import io.github.shanqiang.offheap.buffer.ByteBufferOffheap;
import io.github.shanqiang.offheap.buffer.IntBufferOffheap;
import io.github.shanqiang.offheap.InternalUnsafe;

import static io.github.shanqiang.offheap.InternalUnsafe.copyMemory;
import static io.github.shanqiang.offheap.InternalUnsafe.getLong;
import static io.github.shanqiang.offheap.InternalUnsafe.putLong;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class IntColumn implements ColumnInterface<Integer> {
    private IntBufferOffheap values;
    private ByteBufferOffheap valueIsNull;
    private long size;
    private long capacity;

    IntColumn() {
    }

    public IntColumn(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException();
        }
        this.capacity = capacity;
        values = new IntBufferOffheap(capacity);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long serializeSize() {
        long len = Long.BYTES + size * Integer.BYTES;
        if (null != valueIsNull) {
            len += size * Byte.BYTES;
        }
        return len;
    }

    @Override
    public void serialize(byte[] bytes, long offset, long length) {
        long end = offset + length;
        offset += ARRAY_BYTE_BASE_OFFSET;
        InternalUnsafe.putLong(bytes, offset, size);
        offset += Long.BYTES;

        long len = size * Integer.BYTES;
        InternalUnsafe.copyMemory(null, values.getAddr(), bytes, offset, len);
        offset += len;

        if (null != valueIsNull) {
            len = size * Byte.BYTES;
            InternalUnsafe.copyMemory(null, valueIsNull.getAddr(), bytes, offset, len);
            offset += len;
        }

        if (offset - ARRAY_BYTE_BASE_OFFSET != end) {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public void deserialize(byte[] bytes, long offset, long length) {
        long end = offset + length;

        size = InternalUnsafe.getLong(bytes, offset);
        offset += Long.BYTES;

        capacity = size;
        values = new IntBufferOffheap(capacity);
        long len = size * Integer.BYTES;
        InternalUnsafe.copyMemory(bytes, offset, null, values.getAddr(), len);
        offset += len;

        if (offset < end) {
            len = size * Byte.BYTES;
            valueIsNull = new ByteBufferOffheap(capacity);
            InternalUnsafe.copyMemory(bytes, offset, null, valueIsNull.getAddr(), len);
            offset += len;
        }

        if (offset != end) {
            throw new IndexOutOfBoundsException();
        }
    }

    private void grow() {
        if (size > capacity) {
            throw new IllegalStateException();
        }
        if (size == capacity) {
            capacity = ArrayUtil.calculateNewSize(capacity);
            if (null != valueIsNull) {
                valueIsNull = valueIsNull.copy(capacity);
                valueIsNull.init0(size);
            }

            values = values.copy(capacity);
        }
    }

    @Override
    public void add(Integer value) {
        grow();
        if (null == value) {
            if (null == valueIsNull) {
                valueIsNull = new ByteBufferOffheap(capacity);
                valueIsNull.init();
            }
            valueIsNull.set(size, (byte) 1);
        } else {
            values.set(size, value);
        }

        size++;
    }

    @Override
    public Integer get(int index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        if (index >= size) {
            throw new IndexOutOfBoundsException();
        }

        if (valueIsNull != null && valueIsNull.get(index) == 1) {
            return null;
        }

        return values.get(index);
    }
}
