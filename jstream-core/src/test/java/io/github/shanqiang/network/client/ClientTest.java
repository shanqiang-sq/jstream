package io.github.shanqiang.network.client;

import org.junit.Test;

import javax.net.ssl.SSLException;
import java.time.Duration;

public class ClientTest {
    @Test
    public void test() throws SSLException, InterruptedException {
        int total = 500;
        Thread[] threads = new Thread[total];
        for (int i = 0; i < total; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10; i++) {
                        try {
                            new Client(false, "127.0.0.1", 38823, Duration.ofSeconds(10));
                        } catch (Throwable t) {
                            System.out.println("retry to connect ");
                        }
                    }
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < total; i++) {
            threads[i].join();
        }

        //上面的代码不应该输出线程泄露导致的OOM异常
    }
}