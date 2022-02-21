package io.github.shanqiang.offheap;

import com.alibaba.tc.table.*;
import io.github.shanqiang.table.ColumnTypeBuilder;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class BigDecimalColumnTest {

    @Test
    public void add() {
        String s1 = "+235235252522352329877969869878798796698698698698698697698698698686986983221.235";
        BigDecimal bigDecimal0 = new BigDecimal(s1);
        String s2 = "-235232.23289989";
        BigDecimal bigDecimal1 = new BigDecimal(s2);

        TableBuilder tableBuilder = new TableBuilder(new ColumnTypeBuilder().column("c1", Type.BIGDECIMAL).build());
        tableBuilder.append(0, bigDecimal0);
        tableBuilder.append(0, new ByteArray(s2));
        Table table = tableBuilder.build();
        assertEquals(table.size(), 2);
        assertEquals(table.getColumn("c1").getBigDecimal(0), bigDecimal0);
        assertEquals(table.getColumn(0).get(1), bigDecimal1);
    }
}