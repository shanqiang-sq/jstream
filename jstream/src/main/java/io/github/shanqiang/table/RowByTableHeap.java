package io.github.shanqiang.table;

import io.github.shanqiang.exception.ColumnNotExistsException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowByTableHeap extends AbstractRow {
    private final LinkedHashMap<String, Integer> columnName2Index;
    private final List<List<Comparable>> table;
    private final int row;

    public RowByTableHeap(LinkedHashMap<String, Integer> columnName2Index, List<List<Comparable>> table, int row) {
        this.columnName2Index = requireNonNull(columnName2Index);
        this.table = table;
        this.row = row;
    }

    @Override
    public Set<String> getColumnNames() {
        return columnName2Index.keySet();
    }

    @Override
    public Comparable getComparable(int index) {
        return table.get(index).get(row);
    }

    @Override
    public Comparable getComparable(String columnName) {
        Integer index = columnName2Index.get(columnName);
        if (null == index) {
            throw new ColumnNotExistsException(format("column '%s' not exists", columnName));
        }
        return getComparable(index);
    }

    @Override
    public int size() {
        return columnName2Index.size();
    }
}
