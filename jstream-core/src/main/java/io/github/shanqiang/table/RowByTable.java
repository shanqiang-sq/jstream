package io.github.shanqiang.table;

import io.github.shanqiang.exception.ColumnNotExistsException;

import java.util.Set;

import static java.lang.String.format;

public class RowByTable extends AbstractRow {
    private final Table table;
    private final int row;

    public RowByTable(Table table, int row) {
        this.table = table;
        this.row = row;
    }

    @Override
    public Set<String> getColumnNames() {
        return table.getColumnIndex().keySet();
    }

    @Override
    public Comparable getComparable(int index) {
        return table.getColumn(index).get(row);
    }

    @Override
    public Comparable getComparable(String columnName) {
        Integer index = table.getIndex(columnName);
        if (null == index) {
            throw new ColumnNotExistsException(format("column '%s' not exists", columnName));
        }
        return getComparable(index);
    }

    @Override
    public int size() {
        return table.getColumns().size();
    }
}
