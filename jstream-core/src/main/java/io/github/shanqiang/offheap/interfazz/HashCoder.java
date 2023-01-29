package io.github.shanqiang.offheap.interfazz;

import io.github.shanqiang.offheap.annotation.StaticMethod;

public interface HashCoder {
    @StaticMethod
    int hashCode(long addr);
}
