package io.github.shanqiang.util;

import org.junit.Test;

import java.text.ParseException;

public class DateUtilTest {
    @Test
    public void test() throws ParseException {
        long ts = DateUtil.parseDateWithZone("2021-08-19 23:10:20 EDT");
        assert ts == 1629429020000L;
    }
}