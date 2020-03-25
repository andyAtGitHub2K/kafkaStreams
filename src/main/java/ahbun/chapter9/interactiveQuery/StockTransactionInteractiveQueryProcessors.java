package ahbun.chapter9.interactiveQuery;

import ahbun.chapter9.restore.StateRestoreHttpReporter;
import ahbun.topology.KsqlAppTopology;
import ahbun.topology.TxByIndustryStream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

/***
 * StockTransactionInteractiveQuery provides a REST API to query live data from three Kafka streams
 * showing results of
 *      1. total transaction by market sector
 *      2. customer purchases of shares per session window
 *      3. total number of shares grouped by stock symbol within a tumbling window of X seconds
 *
 *      Combines Kafka Streams processors with
 *      State Stores and an HTTP server to enable read-only kV-store of a Kafka topic.
 */
public class StockTransactionInteractiveQueryProcessors {
    private static Logger logger = LoggerFactory.getLogger(StockTransactionInteractiveQueryProcessors.class);
    private static String STREAM_CONFIG = "chapter9/conf/interactiveQueryStreamConfig.properties";
    private static String APP_CONFIG = "chapter9/conf/interactiveQueryAppConfig.properties";
    private static String TX_BY_INDUSTRY_APP_CONFIG = "chapter9/conf/interactiveTxByIndustrySectorAppConfig.properties";

    public static void main(String[] argv) throws IOException, InterruptedException {
        String host = "localhost";
        int port = 4567;

        if (argv.length != 2) {
            System.out.println(usage());
            logger.info("query server is running at default localhost:4567");
        } else {
            host = argv[0];
            port = Integer.parseInt(argv[1]);
        }

        // load properties for stream and application
        HostInfo hostInfo = new HostInfo(host, port);
        Properties streamProperties = loadStreamProperties(hostInfo);

        // create topology
        Properties appProperties = new Properties();
        appProperties.load(
                Objects.requireNonNull(ClassLoader.getSystemResourceAsStream(APP_CONFIG)));
        //String appPropertiesPath = ClassLoader.getSystemResource(APP_CONFIG).getPath();
        logger.info("app properties path {}", appProperties);
        KsqlAppTopology ksqlAppTopology = new KsqlAppTopology(APP_CONFIG);

        // process tx by Industry
        //Properties txByIndustryAppProperties = new Properties();
        //txByIndustryAppProperties.load(
        //        Objects.requireNonNull(ClassLoader.getSystemResourceAsStream(TX_BY_INDUSTRY_APP_CONFIG)));
        attachTxByIndustryStream(ksqlAppTopology, ClassLoader.getSystemResource(TX_BY_INDUSTRY_APP_CONFIG).getPath());


        Topology topology = ksqlAppTopology.build();
        logger.info(topology.describe().toString());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProperties);

        // b. start up web server to handle REST requests
        InteractiveServer queryServer = new InteractiveServer(kafkaStreams, hostInfo);

        // c. configure Kakfa Streams
        StateRestoreHttpReporter stateRestoreHttpReporter = new StateRestoreHttpReporter(queryServer);
        kafkaStreams.setGlobalStateRestoreListener(stateRestoreHttpReporter);

        enableQueryStateFromWebServer(kafkaStreams, queryServer);


        kafkaStreams.setUncaughtExceptionHandler((t, e) -> {
            logger.error("Thread {} got exception {}", t, e.getMessage());
            shutdown(kafkaStreams, queryServer);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("shutting down application");
            System.out.println("********************");
            shutdown(kafkaStreams, queryServer);
        }));

        logger.info("set server to ready");
        queryServer.setReady(true);
        // d. start streaming
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        logger.info("sleeping for 10 seconds");
        Thread.sleep(50000);
    }

    private static String usage() {
        return "StockTransactionInteractiveQuery <host-name> <port>";
    }

    private static Properties loadStreamProperties(HostInfo hostInfo) throws IOException {
        Properties streamProperties = new Properties();
        streamProperties.load(
                Objects.requireNonNull(ClassLoader.getSystemResourceAsStream(STREAM_CONFIG)));

        streamProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG,
                String.format("%s:%d",hostInfo.host(), hostInfo.port()));
        streamProperties.put(StreamsConfig.topicPrefix("retention.bytes"), 1024 * 1024);
        streamProperties.put(StreamsConfig.topicPrefix("retention.ms"), 3600000);

        return streamProperties;
    }

    private static void attachTxByIndustryStream(
                                                 KsqlAppTopology ksqlAppTopology,
                                                 String appConfigFullPath) {
        // Tx by Industry Sector
        TxByIndustryStream txByIndustryStream = new TxByIndustryStream(appConfigFullPath);
        ksqlAppTopology.attachProcessorStream(txByIndustryStream);
    }

    private static void enableQueryStateFromWebServer(KafkaStreams kafkaStreams, InteractiveServer queryServer) {
        kafkaStreams.setStateListener(((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING &&
                    oldState == KafkaStreams.State.REBALANCING) {
                queryServer.setReady(true);
                logger.info("Kafka Streams is running. query server is ready");
            } else if (newState != KafkaStreams.State.RUNNING) {
                queryServer.setReady(false);
                logger.info("Kafka Streams is not running. query server is unavailable");
            }
        }));
    }

    private static void shutdown(KafkaStreams kafkaStreams, InteractiveServer queryServer) {
        kafkaStreams.close();
        queryServer.stop();
    }
}
