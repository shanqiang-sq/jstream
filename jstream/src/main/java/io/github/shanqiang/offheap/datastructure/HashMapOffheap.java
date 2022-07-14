package io.github.shanqiang.offheap.datastructure;

import io.github.shanqiang.offheap.InternalUnsafe;
import io.github.shanqiang.offheap.interfazz.Offheap;
import io.github.shanqiang.offheap.interfazz.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static io.github.shanqiang.offheap.InternalUnsafe.getAndAddLong;
import static io.github.shanqiang.offheap.InternalUnsafe.getInt;
import static io.github.shanqiang.offheap.InternalUnsafe.getLong;
import static io.github.shanqiang.offheap.InternalUnsafe.getShort;
import static io.github.shanqiang.offheap.InternalUnsafe.putInt;
import static io.github.shanqiang.offheap.InternalUnsafe.putLong;
import static io.github.shanqiang.offheap.InternalUnsafe.putShort;
import static io.github.shanqiang.offheap.InternalUnsafe.setMemory;

public class HashMapOffheap<K extends Offheap<K>, V extends Serializer<V>>
        implements Iterable<K>, Serializer<HashMapOffheap>
{
    private static final Logger logger = LoggerFactory.getLogger(HashMapOffheap.class);

    private static class PreKey {
        long next;
        int hash;
        short keyClassId;
        long value;
        short valueClassId;

        private static final Property[] properties = new Property[5];
        private static final int INSTANCE_SIZE = (int) Serializer.initProperties(PreKey.class, properties);
        private static long nextOffset;
        private static long hashOffset;
        private static long keyClassIdOffset;
        private static long valueOffset;
        private static long valueClassIdOffset;
        static {
            for (int i = 0; i < properties.length; i++) {
                long offset = properties[i].offsetOffheap;
                switch (properties[i].name) {
                    case "next":
                        nextOffset = offset;
                        break;
                    case "hash":
                        hashOffset = offset;
                        break;
                    case "keyClassId":
                        keyClassIdOffset = offset;
                        break;
                    case "value":
                        valueOffset = offset;
                        break;
                    case "valueClassId":
                        valueClassIdOffset = offset;
                        break;
                }
            }
        }

        static long getNext(long addr) {
            return getLong(addr + nextOffset);
        }

        static void setNext(long addr, long next) {
            putLong(addr + nextOffset, next);
        }

        static int getHash(long addr) {
            return getInt(addr + hashOffset);
        }

        static void setHash(long addr, int hash) {
            putInt(addr + hashOffset, hash);
        }

        static short getKeyClassId(long addr) {
            return getShort(addr + keyClassIdOffset);
        }

        static void setKeyClassId(long addr, short keyClassId) {
            putShort(addr + keyClassIdOffset, keyClassId);
        }

        static long getValue(long addr) {
            return getLong(addr + valueOffset);
        }

        static void setValue(long addr, long value) {
            putLong(addr + valueOffset, value);
        }

        static short getValueClassId(long addr) {
            return getShort(addr + valueClassIdOffset);
        }

        static void setValueClassId(long addr, short valueClassId) {
            putShort(addr + valueClassIdOffset, valueClassId);
        }
    }

    private static class Handle {
        private long table = 0L;
        private int size = 0;
        private int capacity = 0;
        private int threshold = 0;
        private long refCount;
        private long finalizeCount;
        private final long addr;

        private static final Property[] properties = new Property[6];
        private static final long INSTANCE_SIZE = Serializer.initProperties(Handle.class, properties);
        private static long tableOffset;
        private static long sizeOffset;
        private static long capacityOffset;
        private static long thresholdOffset;
        private static long refCountOffset;
        private static long finalizeCountOffset;
        static {
            for (int i = 0; i < properties.length; i++) {
                long offset = properties[i].offsetOffheap;
                switch (properties[i].name) {
                    case "table":
                        tableOffset = offset;
                        break;
                    case "size":
                        sizeOffset = offset;
                        break;
                    case "capacity":
                        capacityOffset = offset;
                        break;
                    case "threshold":
                        thresholdOffset = offset;
                        break;
                    case "refCount":
                        refCountOffset = offset;
                        break;
                    case "finalizeCount":
                        finalizeCountOffset = offset;
                        break;
                }
            }
        }

        private Handle(long addr) {
            this.addr = addr;
        }

        void free() {
            InternalUnsafe.free(table());
            InternalUnsafe.free(addr);
        }

        long table() {
            return getLong(addr + tableOffset);
        }

        void table(long newTable) {
            putLong(addr + tableOffset, newTable);
        }

        int size() {
            return getInt(addr + sizeOffset);
        }

        void size(int size) {
            putInt(addr + sizeOffset, size);
        }

        int capacity() {
            return getInt(addr + capacityOffset);
        }

        void capacity(int capacity) {
            putInt(addr + capacityOffset, capacity);
        }

        int threshold() {
            return getInt(addr + thresholdOffset);
        }

        void threshold(int threshold) {
            putInt(addr + thresholdOffset, threshold);
        }

        long getRefCount() {
            return getLong(addr + refCountOffset);
        }

        long getAndAddRefCount(long l) {
            return getAndAddLong(addr + refCountOffset, l);
        }

        long getFinalizeCount() {
            return getLong(addr + finalizeCountOffset);
        }

        long getAndAddFinalizeCount(long l) {
            return getAndAddLong(addr + finalizeCountOffset, l);
        }
    }

    private final Handle handle;
    private volatile boolean released;
    // 大部分情况下key和value的类型加起来只有2个
    // 但可以是任意多的key类型和value类型只要满足派生条件
    private final List<Object> prototypeList = new ArrayList<>(2);

    /**
     * The default initial capacity - MUST be a power of two.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The load factor used when none specified in constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private HashMapOffheap(Handle handle) {
        this.handle = handle;
    }

    @Override
    public HashMapOffheap deserialize(long addr)
    {
        return new HashMapOffheap(new Handle(getLong(addr)));
    }

    @Override
    public long allocAndSerialize(int extraSize)
    {
        checkReleased();
        handle.getAndAddRefCount(1);
        handle.getAndAddFinalizeCount(1);
        long addr = InternalUnsafe.alloc(extraSize + Long.BYTES);
        putLong(addr + extraSize, handle.addr);
        return addr;
    }

    @Override
    public void free(long addr, int extraSize)
    {
        HashMapOffheap hashMapOffheap = deserialize(addr + extraSize);
        long count = hashMapOffheap.handle.getAndAddRefCount(-1);
        if (count < 1) {
            throw new IllegalStateException("already released, count: " + count);
        }
        if (count == 1) {
            hashMapOffheap.free();
        }
        InternalUnsafe.free(addr);
    }

    public HashMapOffheap()
    {
        handle = new Handle(InternalUnsafe.alloc(Handle.INSTANCE_SIZE));
        setMemory(handle.addr, Handle.INSTANCE_SIZE, (byte) 0);
        handle.getAndAddRefCount(1);
        handle.getAndAddFinalizeCount(1);
        resize();
    }

    @Override
    public Iterator<K> iterator() {
        checkReleased();
        return new KeyIterator();
    }

    /**
     * Computes key.hashCode() and spreads (XORs) higher bits of hash
     * to lower.  Because the table uses power-of-two masking, sets of
     * hashes that vary only in bits above the current mask will
     * always collide. (Among known examples are sets of Float keys
     * holding consecutive whole numbers in small tables.)  So we
     * apply a transform that spreads the impact of higher bits
     * downward. There is a tradeoff between speed, utility, and
     * quality of bit-spreading. Because many common sets of hashes
     * are already reasonably distributed (so don't benefit from
     * spreading), and because we use trees to handle large sets of
     * collisions in bins, we just XOR some shifted bits in the
     * cheapest possible way to reduce systematic lossage, as well as
     * to incorporate impact of the highest bits that would otherwise
     * never be used in index calculations because of table bounds.
     */
    private final int hash(K key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    private K prototypeKey(long addr)
    {
        return (K) prototypeList.get(PreKey.getKeyClassId(addr));
    }

    private V prototypeValue(long addr)
    {
        return (V) prototypeList.get(PreKey.getValueClassId(addr));
    }

    private void deleteElement(long addr)
    {
        prototypeValue(addr).free(PreKey.getValue(addr), 0);
        prototypeKey(addr).free(addr, PreKey.INSTANCE_SIZE);
    }

    public boolean isEmpty()
    {
        checkReleased();
        return 0 == handle.size();
    }

    private boolean resize()
    {
        if (handle.size() < handle.threshold()) {
            return false;
        }

        int oldCap = handle.capacity();
        int oldThr = handle.threshold();
        int newCap, newThr = 0;
        if (oldCap > 0) {
            if (oldCap >= MAXIMUM_CAPACITY) {
                handle.threshold(Integer.MAX_VALUE);
                return false;
            }
            if ((newCap = oldCap << 1) < MAXIMUM_CAPACITY &&
                    oldCap >= DEFAULT_INITIAL_CAPACITY) {
                // double threshold
                newThr = oldThr << 1;
            }
        }
        else {
            // zero initial threshold signifies using defaults
            newCap = DEFAULT_INITIAL_CAPACITY;
            newThr = (int)(DEFAULT_LOAD_FACTOR * DEFAULT_INITIAL_CAPACITY);
        }

        handle.threshold(newThr);
        handle.capacity(newCap);
        long oldTable = handle.table();
        long size = Long.BYTES * (long) newCap;
        handle.table(InternalUnsafe.alloc(size));
        setMemory(handle.table(), size, (byte) 0);
        for (int i = 0; i < oldCap; i++) {
            long head = getLong(oldTable + i * Long.BYTES);
            while (0L != head) {
                long next = getLong(head);
                add(head);
                head = next;
            }
        }
        InternalUnsafe.free(oldTable);

        return true;
    }

    private void add(long addr)
    {
        int hash = PreKey.getHash(addr);
        int position = position(hash);
        long head = getHead(position);
        PreKey.setNext(addr, head);
        setHead(position, addr);
    }

    private long getHead(int position)
    {
        return getLong(handle.table() + position * Long.BYTES);
    }

    private void setHead(int position, long addr)
    {
        putLong(handle.table() + position * Long.BYTES, addr);
    }

    private int position(int hash)
    {
        return hash & (handle.capacity() - 1);
    }

    private short genClassId(Serializer o) {
        for (short i = 0; i < prototypeList.size(); i++) {
            if (prototypeList.get(i).getClass() == o.getClass()) {
                return i;
            }
        }
        prototypeList.add(o);
        if (prototypeList.size() > 64) {
            throw new IllegalStateException("too many different classes, size: " + prototypeList.size());
        }
        return (short) (prototypeList.size() - 1);
    }

    public V put(K key, V value)
    {
        checkReleased();
        if (null == key) {
            throw new NullPointerException();
        }
        if (null == value) {
            throw new NullPointerException();
        }

        long valueAddr = value.allocAndSerialize(0);
        short valueClassId = genClassId(value);

        int h = hash(key);
        int position = position(h);
        long head = getHead(position);
        long p = head;
        while (0L != p) {
            if (0 == key.compareTo(p + PreKey.INSTANCE_SIZE)) {
                V ret = prototypeValue(p).deserialize(PreKey.getValue(p));
                prototypeValue(p).free(PreKey.getValue(p), 0);
                PreKey.setValueClassId(p, valueClassId);
                PreKey.setValue(p, valueAddr);
                return ret;
            }
            p = PreKey.getNext(p);
        }

        long addr = key.allocAndSerialize(PreKey.INSTANCE_SIZE);
        PreKey.setNext(addr, head);
        PreKey.setHash(addr, h);
        PreKey.setKeyClassId(addr, genClassId(key));
        PreKey.setValueClassId(addr, valueClassId);
        PreKey.setValue(addr, valueAddr);
        setHead(position, addr);

        handle.size(handle.size() + 1);
        resize();

        return null;
    }

    private boolean contains(long head, K key)
    {
        while (0L != head) {
            if (0 == key.compareTo(head + PreKey.INSTANCE_SIZE)) {
                return true;
            }
            head = PreKey.getNext(head);
        }
        return false;
    }

    public boolean contains(K key)
    {
        checkReleased();
        int position = position(hash(key));
        long head = getHead(position);
        return contains(head, key);
    }

    private V get(long head, K key)
    {
        while (0L != head) {
            if (0 == key.compareTo(head + PreKey.INSTANCE_SIZE)) {
                return prototypeValue(head).deserialize(PreKey.getValue(head));
            }
            head = PreKey.getNext(head);
        }
        return null;
    }

    public int size() {
        return handle.size();
    }

    public V get(K key)
    {
        checkReleased();
        int position = position(hash(key));
        long head = getHead(position);
        return get(head, key);
    }

    public boolean remove(K key)
    {
        checkReleased();
        int position = position(hash(key));
        long pre = getHead(position);
        long p = pre;
        pre = 0L;
        while (0L != p) {
            long next = PreKey.getNext(p);
            if (0 == key.compareTo(p + PreKey.INSTANCE_SIZE)) {
                if (0L == pre) {
                    setHead(position, next);
                }
                else {
                    PreKey.setNext(pre, next);
                }
                deleteElement(p);
                handle.size(handle.size() - 1);
                return true;
            }
            pre = p;
            p = next;
        }
        return false;
    }

    private void freeInternal()
    {
        for (int i = 0; i < handle.capacity(); i++) {
            long head = getHead(i);
            while (0L != head) {
                long next = PreKey.getNext(head);
                deleteElement(head);
                head = next;
            }
        }
        handle.free();
        released = true;
    }

    private void checkReleased() {
        if (released) {
            throw new IllegalStateException("try to use a released object");
        }
    }

    public void free()
    {
        checkReleased();
        long count = handle.getAndAddRefCount(-1);
        if (count != 1) {
            throw new IllegalStateException("should not free a using object, count: " + count);
        }

        freeInternal();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (released) {
            return;
        }

        long count = handle.getAndAddFinalizeCount(-1);
        if (count > 1) {
            return;
        }
        if (count <= 0) {
            throw new IllegalStateException("should not be 0 or negative, count: " + count);
        }

        count = handle.getRefCount();
        if (0 != count) {
            logger.warn("not free before finalize, count: " + count);
            freeInternal();
        }
    }

    private final class KeyIterator implements Iterator<K> {
        long next;
        final int expectedSize;
        int index;

        KeyIterator()
        {
            expectedSize = handle.size();
            next = 0L;
            index = 0;
            if (handle.table() != 0L && handle.size() > 0) { // advance to first entry
                do {} while (index < handle.capacity() && (next = getHead(index++)) == 0L);
            }
        }

        @Override
        public boolean hasNext() {
            return next != 0L;
        }

        @Override
        public K next() {
            long k = next;
            if (handle.size() != expectedSize) {
                throw new ConcurrentModificationException();
            }
            if (k == 0L) {
                throw new NoSuchElementException();
            }
            if ((next = PreKey.getNext(k)) == 0L && handle.table() != 0L) {
                do {} while (index < handle.capacity() && (next = getHead(index++)) == 0L);
            }
            return prototypeKey(k).deserialize(k + PreKey.INSTANCE_SIZE);
        }
    }
}
