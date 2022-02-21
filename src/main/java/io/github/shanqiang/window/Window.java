package io.github.shanqiang.window;

import io.github.shanqiang.table.Row;
import io.github.shanqiang.table.Table;

import java.util.Arrays;
import java.util.List;

public abstract class Window {
    static void checkTablesSize(List<Table> tables) {
        if (tables.size() < 1) {
            throw new IllegalArgumentException("tables.size at least 1, please use Rehash.rehash to get tables");
        }
    }

    public List<Row> getRows(Comparable... partitionBy) {
        return getRows(Arrays.asList(partitionBy));
    }

    public abstract List<Row> getRows(List<Comparable> partitionBy);
}
