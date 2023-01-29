package io.github.shanqiang.criteria;

import io.github.shanqiang.table.Row;

public interface Criteria {
    boolean filter(Row row);
}
