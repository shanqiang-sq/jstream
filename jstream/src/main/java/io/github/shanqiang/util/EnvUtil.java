package io.github.shanqiang.util;

import java.io.IOException;
import java.util.Properties;

public class EnvUtil {
    private static Properties properties = null;

    public static synchronized String getEnvProperty(String key) {
        if (null == properties) {
            String env = System.getProperty("env");
            if (null == env) {
                throw new IllegalArgumentException("unspecified -Denv param, use like \"java -Denv=prd ...\" to run");
            }

            properties = new Properties();
            try {
                properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(env + ".properties"));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return properties.getProperty(key);
    }
}
