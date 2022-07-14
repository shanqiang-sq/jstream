package io.github.shanqiang.offheap.interfazz;

import static io.github.shanqiang.offheap.InternalUnsafe.getByte;
import static io.github.shanqiang.offheap.InternalUnsafe.getLong;
import static java.lang.Math.min;

public interface ComparableOffheap<T> extends Comparable<T> {
    int compareTo(long addr);

    public static int compareTo(Object thisObj, long thisAddr, int thisSize, Object thatObj, long thatAddr, int thatSize)
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

    static long longBytesToLong(long bytes) {
        return Long.reverseBytes(bytes) ^ 0x8000000000000000L;
    }
}
