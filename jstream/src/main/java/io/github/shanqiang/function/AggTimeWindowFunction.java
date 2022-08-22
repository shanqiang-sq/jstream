package io.github.shanqiang.function;

import io.github.shanqiang.table.Row;

import java.util.List;

public interface AggTimeWindowFunction {
    // 窗口区间： [windowStart, windowEnd)

    /**
     * 首次调用时传入的preAggResult为null
     * 返回的Comparable[]跟最终Table的列不需要一致，最终Table的列以aggEnd返回的结果为准
     * @param preAggResult
     * @param partitionByColumns
     * @param newRow
     * @param windowStart
     * @param windowEnd
     * @return
     */
    Comparable[] agg(Comparable[] preAggResult, List<Comparable> partitionByColumns, Row newRow, long windowStart, long windowEnd);

    /**
     * 返回的Comparable[]必须跟最终Table的列一致
     * 比如最终的Table只想要avg一列，但通过agg聚合的时候我们要同时累加sum和count，而最终的avg通过aggEnd返回
     * agg的返回值可能comparable[0]代表sum，comparable[1]代表count，通过aggEnd只返回comparable[0]代表avg做为最终Table的列
     * @param preAggResult
     * @param partitionByColumns
     * @param windowStart
     * @param windowEnd
     * @return
     */
    default Comparable[] aggEnd(Comparable[] preAggResult, List<Comparable> partitionByColumns, long windowStart, long windowEnd) {
        return preAggResult;
    }
}
