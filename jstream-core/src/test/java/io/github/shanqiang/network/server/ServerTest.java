package io.github.shanqiang.network.server;

import io.github.shanqiang.JStream;
import io.github.shanqiang.SystemProperty;
import io.github.shanqiang.network.client.Client;
import io.github.shanqiang.sp.Rehash;
import io.github.shanqiang.sp.StreamProcessing;
import io.github.shanqiang.table.ColumnTypeBuilder;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import org.junit.Test;

import javax.net.ssl.SSLException;
import java.time.Duration;
import java.util.Map;

public class ServerTest {
    @Test
    public void test() throws SSLException, InterruptedException {
        System.setProperty("self", "127.0.0.1:8823");
        System.setProperty("all", "127.0.0.1:8823");
        SystemProperty.init();
        JStream.stopServer();
        StreamProcessing streamProcessing = new StreamProcessing(1);
        // 等待StreamProcessing构造函数中的JStream.startServer完成启动
        Thread.sleep(3000);

        String uniqueName = "rehash";
        Rehash rehash = new Rehash(streamProcessing
                , 1
                , 10000_0000
                , 1000_0000
                , true
                , uniqueName
                , "c1");
        Map<String, Type> columnTypeMap = new ColumnTypeBuilder()
                .column("c1", Type.INT)
                .column("c2", Type.VARBYTE)
                .column("c3", Type.BIGINT)
                .build();
        TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
        for (int i = 0; i < 10000; i++) {
            tableBuilder.append(0, 1);
            tableBuilder.append(1, "c2v1");
            tableBuilder.append(2, System.currentTimeMillis());
        }
        Table table = tableBuilder.build();

        int total = 100;
        Thread[] threads = new Thread[total];
        for (int i = 0; i < total; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Client client = new Client(false, "127.0.0.1", 8823, Duration.ofSeconds(10));
                        for (int j = 0; j < 10; j++) {
                            client.asyncRequest("rehash", uniqueName, 0, table);
                        }
//                        client.close();
                    } catch (SSLException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < total; i++) {
            threads[i].join();
        }

        Table table1 = rehash.consumeBatch(0);
        assert table1.size() == 1000_0000;
        //上面的代码不抛异常即为通过测试
    }
}