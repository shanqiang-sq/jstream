package io.github.shanqiang.window;

import io.github.shanqiang.table.Row;
import io.github.shanqiang.table.Table;

import java.util.Arrays;
import java.util.List;

public abstract class Window {
    public List<Row> getRows(Comparable... partitionBy) {
        return getRows(Arrays.asList(partitionBy));
    }

    public abstract List<Row> getRows(List<Comparable> partitionBy);
}
