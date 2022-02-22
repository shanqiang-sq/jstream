package io.github.shanqiang.sp;

public interface Compute {
    void compute(int myThreadIndex) throws InterruptedException;
}
