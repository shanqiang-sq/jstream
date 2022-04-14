package io.github.shanqiang.sp.dimension;

import io.github.shanqiang.table.Index;
import io.github.shanqiang.table.Row;
import io.github.shanqiang.table.RowByTable;
import io.github.shanqiang.table.Table;

import java.util.List;

public class TableIndex {
    private final Table table;
    private final Index index;

    public TableIndex(Table table, Index index) {
        this.table = table;
        this.index = index;
    }

    public Table getTable() {
        return table;
    }

    public Index getIndex() {
        return index;
    }

    public List<Integer> getRows(Comparable... primaryKey) {
        return index.get(primaryKey);
    }

    public Row getRow(Comparable... primaryKey) {
        List<Integer> rows = getRows(primaryKey);
        if (null == rows || rows.size() < 1) {
            return null;
        }
        return new RowByTable(table, rows.get(0));
    }
}
