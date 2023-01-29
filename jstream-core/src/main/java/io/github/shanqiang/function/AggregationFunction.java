package io.github.shanqiang.function;

import io.github.shanqiang.table.Row;

import java.util.List;

public interface AggregationFunction {
    Comparable[] agg(List<Comparable> groupByColumns, List<Row> rows);
}
