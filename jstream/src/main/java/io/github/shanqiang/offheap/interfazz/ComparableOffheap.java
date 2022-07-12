package io.github.shanqiang.offheap.interfazz;

public interface ComparableOffheap<T> extends Comparable<T> {
    int compareTo(long addr);
}
