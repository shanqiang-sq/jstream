package io.github.shanqiang.criteria;

import io.github.shanqiang.table.Row;

import java.util.List;

public interface JoinCriteria {
    List<Integer> theOtherRows(Row thisRow);
}
