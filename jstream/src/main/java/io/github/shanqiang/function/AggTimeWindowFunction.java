package io.github.shanqiang.function;

import io.github.shanqiang.table.Row;

import java.util.List;

public interface AggTimeWindowFunction {
    // 窗口区间： [windowStart, windowEnd)

    /**
     * 必须拿到窗口内的所有行之后才能进行聚合的场景使用这个接口函数
     * 注意：窗口很大的情况下窗口里的数据都要先压在内存里等窗口闭合的时候一次性传到这个函数里，这可能导致频繁GC甚至OOM
     * 因此尽可能采用AggTimeWindowFunction进行聚合
     * @param partitionByColumns
     * @param rows
     * @param windowStart
     * @param windowEnd
     * @return
     */
    Comparable[] agg(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd);
}
