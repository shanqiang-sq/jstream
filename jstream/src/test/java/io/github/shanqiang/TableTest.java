package io.github.shanqiang;

import io.github.shanqiang.exception.InconsistentColumnTypeException;
import io.github.shanqiang.function.OverWindowFunction;
import io.github.shanqiang.offheap.ByteArray;
import io.github.shanqiang.table.As;
import io.github.shanqiang.table.Column;
import io.github.shanqiang.table.ColumnTypeBuilder;
import io.github.shanqiang.table.Index;
import io.github.shanqiang.table.Row;
import io.github.shanqiang.table.RowByTable;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import io.github.shanqiang.util.ScalarUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.github.shanqiang.util.AggregationUtil.groupConcat;
import static io.github.shanqiang.util.OrderByWindowUtil.rank;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.stream.Collectors.toList;

public class TableTest {
    @Test
    public void serialize() {
        TableBuilder tableBuilder = new TableBuilder(new ColumnTypeBuilder().
                column("long", Type.BIGINT).
                column("long1", Type.BIGINT).
                build());
        tableBuilder.append(0, 1L);
        tableBuilder.append(0, 2L);
        tableBuilder.append(0, 3L);
        tableBuilder.append(1, 1L);
        tableBuilder.append(1, 2L);
        tableBuilder.append(1, 3L);
        byte[] bytes = tableBuilder.build().serialize();
        Table table = Table.deserialize(bytes);
        assert table.getColumn("long").get(0).equals(1L);
        assert table.getColumn("long").get(1).equals(2L);
        assert table.getColumn("long").get(2).equals(3L);
        assert table.getColumn("long1").get(0).equals(1L);
        assert table.getColumn("long1").get(1).equals(2L);
        assert table.getColumn("long1").get(2).equals(3L);
    }

    @Test
    public void serializeVarchar() {
        TableBuilder tableBuilder = new TableBuilder(new ColumnTypeBuilder().
                column("str", Type.VARBYTE).
                column("long", Type.BIGINT).
                build());
        tableBuilder.append(0, "1L");
        tableBuilder.append(0, "2L");
        tableBuilder.append(0, "3L");
        tableBuilder.appendValue(0, null);
        tableBuilder.append(1, 1L);
        tableBuilder.append(1, 2L);
        tableBuilder.append(1, 3L);
        tableBuilder.appendValue(1, null);
        byte[] bytes = tableBuilder.build().serialize();
        Table table = Table.deserialize(bytes);
        assert table.getColumn("long").get(0).equals(1L);
        assert table.getColumn("long").get(1).equals(2L);
        assert table.getColumn("long").get(2).equals(3L);
        assert table.getColumn("long").get(3) == null;
        assert table.getColumn("str").get(0).equals(new ByteArray("1L"));
        assert table.getColumn("str").get(1).equals(new ByteArray("2L"));
        assert table.getColumn("str").get(2).equals(new ByteArray("3L"));
        assert table.getColumn("str").get(3) == null;
    }

    @Test
    public void createIndex() throws InterruptedException {
        Column column1 = new Column("c1");
        Column column2 = new Column("c2");
        column1.add(1);
        column1.add(-1);
        column1.add(null);
        column1.add(1);

        column2.add("ss");
        column2.add(null);
        column2.add("dd");
        column2.add(null);

        String message = "";
        try {
            column1.add(1L);
        } catch (InconsistentColumnTypeException e) {
            message = e.getMessage();
        }
        assert !isNullOrEmpty(message);

        Table table = new Table(new ArrayList<Column>() {{
            add(column1);
            add(column2);
        }});


        Index index = table.createIndex("c1");
        List<Integer> rows = index.getColumns2Rows().get(new ArrayList<Integer>() {{
            add(-1);
        }});
        assert rows.get(0).equals(1);
        rows = index.getColumns2Rows().get(new ArrayList<Integer>() {{
            add(1);
        }});
        assert rows.get(0).equals(0);
        rows = index.getColumns2Rows().get(new ArrayList<Integer>() {{
            add(null);
        }});
        assert rows.get(0).equals(2);

        table = table.groupBy(null, (groupByColumns, innerRows) -> {
            return new Comparable[]{
                    innerRows.size(),
                    String.join(",", innerRows.stream().map(row -> row.getString("c2")).collect(toList())),
                    groupConcat(innerRows, "c2")
            };
        }, new String[]{"c1"}, "cn", "ss", "ss_group_concat");
        assert new RowByTable(table, 0).getInteger("cn").equals(2);
        assert new RowByTable(table, 0).getString("ss").equals("ss,null");
        assert new RowByTable(table, 0).getString("ss_group_concat").equals("ss,null");
        assert new RowByTable(table, 1).getString("ss").equals("null");

        Table table1 = table.select((Row row) -> {
            return new Comparable[]{
                    row.getComparable("c1"),
                    row.getComparable("cn"),
                    ScalarUtil.substr(row.getString("ss"), 0, 2)
            };
        }, false, "c1", "count", "group_concat");
        assert new RowByTable(table1, 1).getString("group_concat").equals("nu");
    }

    @Test
    public void over() {
        Column column1 = new Column("c1");
        Column column2 = new Column("c2");
        column1.add(1);
        column1.add(-1);
        column1.add(null);
        column1.add(1);

        column2.add("ss");
        column2.add(null);
        column2.add("dd");
        column2.add("cc");

        Table table = new Table(new ArrayList<Column>() {{
            add(column1);
            add(column2);
        }});

        table = table.over(null, new OverWindowFunction() {
            @Override
            public List<Comparable[]> transform(List<Comparable> partitionByColumns, List<Row> rows) {
                List<Comparable[]> comparablesList = new ArrayList<>(rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    comparablesList.add(new Comparable[]{rank(i)});
                }
                return comparablesList;
            }
        }, new String[]{"c1"}, new String[]{"c2"}, "rank");
        assert table.size() == 4;
        assert (int) table.getColumn("rank").get(1) == 2;
        table.print();
    }

    @Test
    public void leftJoin() {
        Column column1 = new Column("c1");
        Column column2 = new Column("c2");
        column1.add(1);
        column1.add(-1);
        column1.add(null);
        column1.add(1);

        column2.add("ss");
        column2.add(null);
        column2.add("dd");
        column2.add("cc");

        Table table1 = new Table(new ArrayList<Column>() {{
            add(column1);
            add(column2);
        }});

        Column column21 = new Column("c3");
        Column column22 = new Column("c4");
        column21.add(1);
        column21.add(null);
        column21.add(1);
        column21.add(2);

        column22.add("ss");
        column22.add("ff");
        column22.add(null);
        column22.add("ee");

        Table table2 = new Table(new ArrayList<Column>() {{
            add(column21);
            add(column22);
        }});

        Index finalIndex = table2.createIndex("c3");
        Table table = table1.leftJoin(table2, (Row row) -> {
            return finalIndex.get(new Comparable[]{row.getComparable("c1")});
        }, new As().build(), new As().as("c3", "t2_c1").as("c4", "t2_c2").build());
        assert table.size() == 6;
        assert table.getColumn(3) != null;
        assert table.getColumn("c1").get(3) == null;
        assert (int) table.getColumn("c1").get(2) == -1;
        assert table.getColumn("c2").get(3).toString().equals("dd");
        assert table.getColumn("t2_c1").get(3) == null;
        assert table.getColumn("t2_c2").get(3).toString().equals("ff");
        table.print();

        table = table1.innerJoin(table2, (Row row) -> {
            return finalIndex.get(new Comparable[]{row.getComparable("c1")});
        }, new As().build(), new As().build());
        assert table.size() == 5;
        table.print();

        table = table1.outerJoin(table2, (Row row) -> {
            return finalIndex.get(new Comparable[]{row.getComparable("c1")});
        }, new As().build(), new As().build());
        assert table.size() == 7;
        table.print();
    }
}