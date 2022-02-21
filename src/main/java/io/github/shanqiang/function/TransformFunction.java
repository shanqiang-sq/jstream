package io.github.shanqiang.function;

import io.github.shanqiang.table.Row;

import java.util.List;

public interface TransformFunction {
    List<Comparable[]> returnMultiRow(Row row);
}
