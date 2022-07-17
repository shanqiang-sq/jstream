package io.github.shanqiang;

import io.github.shanqiang.network.server.Server;
import io.github.shanqiang.sp.Node;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class JStream {
    private static boolean started = false;
    public static synchronized void startServer() {
        if (started) {
            return;
        }
        started = true;

        Node self = SystemProperty.getSelf();
        if (null == self) {
            return;
        }
        Server server = new Server(false
                , self.getHost()
                , self.getPort()
                , Runtime.getRuntime().availableProcessors()
                , Runtime.getRuntime().availableProcessors());
        newSingleThreadExecutor(Threads.threadsNamed("server")).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    server.start();
                } catch (CertificateException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (SSLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
