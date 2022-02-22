package io.github.shanqiang.function;

import io.github.shanqiang.table.Row;

public interface ScalarFunction {
    Comparable[] returnOneRow(Row row);
}
