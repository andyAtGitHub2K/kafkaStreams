package ahbun.chapter9;

import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class H2DbServer {
    private static Logger logger = LoggerFactory.getLogger(H2DbServer.class);

    private final static String[] SERVER_OPTIONS = {"-tcp", "-tcpPort", "9989", "-tcpAllowOthers"};
    private static Server server;
    private static ExecutorService executorService = Executors.newCachedThreadPool();
    public static void startDBServer() {
        Runnable runnable = () -> {
            try {
                server = Server.createTcpServer(SERVER_OPTIONS).start();
            } catch (SQLException ex) {
                logger.error(ex.getMessage());
            }
        };
        executorService.submit(runnable);
    }

    public static void stopDBServer() {
        if (server != null) {
            server.stop();
        }
    }
    public static void shutdown() {
        stopDBServer();
    }

    public static void main(String[] argv) {
        try {
            H2DbServer.startDBServer();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            H2DbServer.shutdown();
        }
    }
}
