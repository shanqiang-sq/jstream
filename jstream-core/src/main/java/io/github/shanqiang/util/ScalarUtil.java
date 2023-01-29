package io.github.shanqiang.util;

import com.google.common.base.CharMatcher;
import org.apache.commons.codec.digest.MurmurHash3;

import java.math.BigDecimal;

public class ScalarUtil {
    public static Integer toInteger(Object object) {
        return null == object ? null : (Integer) object;
    }

    public static Long toLong(Object object) {
        return null == object ? null : (Long) object;
    }

    public static Double toDouble(Object object) {
        return null == object ? null : (Double) object;
    }

    public static String toStr(Object object) {
        return null == object ? null : object.toString();
    }

    public static BigDecimal toBigDecimal(Object object) {
        return null == object ? null : (BigDecimal) object;
    }

    public static String substr(Object object, int begin, int end) {
        if (null == object) {
            return null;
        }

        return ((String) object).substring(begin, end);
    }

    public static String substr(Object object, int begin) {
        if (null == object) {
            return null;
        }

        return ((String) object).substring(begin);
    }

    public static String ltrim(String src, char trim) {
        return CharMatcher.is(trim).trimLeadingFrom(src);
    }

    public static String trim(String src, char trim) {
        return CharMatcher.is(trim).trimFrom(src);
    }

    public static String rtrim(String src, char trim) {
        return CharMatcher.is(trim).trimTrailingFrom(src);
    }

    public static int murmur3Hash32(final byte[] data) {
        return MurmurHash3.hash32(data);
    }
}
