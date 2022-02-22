package io.github.shanqiang.sp.input;

import io.github.shanqiang.table.Table;

public interface StreamTable {
    void start();
    void stop();
    Table consume() throws InterruptedException;
    boolean isFinished();
}
