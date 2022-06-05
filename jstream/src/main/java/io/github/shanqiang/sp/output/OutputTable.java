package io.github.shanqiang.sp.output;

import io.github.shanqiang.table.Table;

public interface OutputTable {
    void start();
    void stop();
    void produce(Table table) throws InterruptedException;
}
