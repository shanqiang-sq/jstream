package io.github.shanqiang.window;

import io.github.shanqiang.function.OverFunction;
import io.github.shanqiang.table.Column;
import io.github.shanqiang.table.Row;
import io.github.shanqiang.table.SortedTable;
import io.github.shanqiang.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class OverWindowBySize extends Window {
    private static final Logger logger = LoggerFactory.getLogger(OverWindowBySize.class);

    private int windowSize;
    private final String[] partitionByColumnNames;
    private final String[] orderByColumnNames;
    private final OverFunction overFunction;
    private final String[] additionalColumns;
    private Map<List<Comparable>, SortedTable> partitionedTables = new ConcurrentHashMap<>();

    public OverWindowBySize(int thread,
                            int windowSize,
                            String[] partitionByColumnNames,
                            String[] orderByColumnNames,
                            OverFunction overFunction,
                            String... additionalColumns) {
        if (thread < 1) {
            throw new IllegalArgumentException();
        }
        if (windowSize < 1) {
            throw new IllegalArgumentException();
        }
        this.windowSize = windowSize;
        this.partitionByColumnNames = requireNonNull(partitionByColumnNames);
        if (partitionByColumnNames.length < 1) {
            throw new IllegalArgumentException("at least one partition by column");
        }
        this.orderByColumnNames = requireNonNull(orderByColumnNames);
        this.overFunction = requireNonNull(overFunction);
        this.additionalColumns = requireNonNull(additionalColumns);
    }

    public Table over(Table hashed) {
        Table table = hashed;
        List<Column> columns = new ArrayList<>(additionalColumns.length);
        for (int i = 0; i < additionalColumns.length; i++) {
            Column column = new Column(additionalColumns[i], table.size());
            columns.add(column);
        }

        for (int i = 0; i < table.size(); i++) {
            List<Comparable> key = TimeWindow.genPartitionKey(table, i, partitionByColumnNames);
            SortedTable partitionedTable = TimeWindow.getPartitionedTable(key, table, partitionedTables, orderByColumnNames);

            partitionedTable.addRow(table, i);

            Comparable[] comparables = overFunction.agg(key, partitionedTable.rows());
            for (int j = 0; j < additionalColumns.length; j++) {
                columns.get(j).add(comparables[j]);
            }

            if (partitionedTable.size() == windowSize) {
                partitionedTable.removeFirstRow();
            }

            if (partitionedTable.size() > windowSize) {
                throw new IllegalStateException();
            }
        }

        table.addColumns(columns);

        return table;
    }

    @Override
    public List<Row> getRows(List<Comparable> partitionBy) {
        SortedTable partitionedTable = partitionedTables.get(partitionBy);
        if (null == partitionedTable) {
            return null;
        }
        return partitionedTable.rows();
    }
}
