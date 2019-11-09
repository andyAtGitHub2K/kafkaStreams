package ahbun.chapter6.processor;

import ahbun.lib.JsonSerializer;
import ahbun.lib.StreamsSerdes;
import ahbun.model.StockPerformance;
import ahbun.model.StockTransaction;
import ahbun.util.KafkaStreamDebugPrinter;
import ahbun.util.MockDataProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class StockPerformanceMonitor {
    private static Logger logger = LoggerFactory.getLogger(StockPerformanceMonitor.class);
    private static DecimalFormat decimalFormat = new DecimalFormat("###.##");

    public static void main(String[] argv) throws IOException {
        Properties streamPproperties =  new Properties();
        streamPproperties.load(Objects
                .requireNonNull(
                        ClassLoader
                                .getSystemResourceAsStream("chapter6/stock_trend_monitor_stream.properties")));

        Properties appProperties = new Properties();
        appProperties.load(Objects
                .requireNonNull(
                        ClassLoader
                                .getSystemResourceAsStream("chapter6/stock_trend_monitor_app.properties")));


        System.out.println(appProperties.getProperty("threshold.pct"));
        String valueChangeThresholdInPCT = decimalFormat.format(Double.parseDouble(appProperties.getProperty("threshold.pct")));
        double threshold = Double.parseDouble(valueChangeThresholdInPCT);
        Predicate<StockPerformance> performancePredicate = s -> {
            boolean update =  s != null &&
                    (Math.abs(s.getVolumeChangePCT()) >= threshold ||
                            Math.abs(s.getPriceChangePCT()) >= threshold);


            if (update && // skip if message was sent previously
                    (s.getSendMessageInstant() == null || s.getLastUpdateInstant().isAfter(s.getSendMessageInstant()))) {
                return true;
            }

            return false;
        };
        KafkaStreams kafkaStreams = new KafkaStreams(createTopology(appProperties, performancePredicate),
                streamPproperties);
        try {
            kafkaStreams.cleanUp();

            MockDataProducer.produceStockTransaction(5, 5,
                    appProperties.getProperty("stock.tx.topic"), 4, 200, null);

            kafkaStreams.start();
            Thread.sleep(Integer.parseInt(appProperties.getProperty("sleep.ms")));
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        } finally {
            kafkaStreams.close();
            MockDataProducer.shutdown();
        }
    }

    private static Topology createTopology(Properties appProperties,
                                           Predicate<StockPerformance> predicate) {
        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> txSerde = StreamsSerdes.StockTransactionSerde();
        Serializer<StockPerformance> stockPerformanceSerializer = new JsonSerializer<>();
        String storePerformanceKVStoreName = appProperties.getProperty("kvstore.name");
        int sampleSize = Integer.parseInt(appProperties.getProperty("sample-size"));
        Duration duration = Duration.ofMillis(Long.parseLong(appProperties.getProperty("process.interval.ms")));

        Supplier<StockPerformance> stockPerformanceSupplier = ()
                -> new StockPerformance(sampleSize);
        ProcessorSupplier<String, StockTransaction> stockPerformanceProcessorSupplier =
                () -> new StockPerformanceProcessor(
                        storePerformanceKVStoreName,
                        duration,
                        stockPerformanceSupplier,
                        predicate);

        // create kv store
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(storePerformanceKVStoreName);
        StoreBuilder<KeyValueStore<String, StockPerformance>> storeBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, stringSerde, StreamsSerdes.StockPerfSerde());

        Topology topology = new Topology();
        topology
                .addSource(
                        (String)appProperties.get("transaction.source.nodename"),
                        stringSerde.deserializer(),
                        txSerde.deserializer(),
                        (String)appProperties.get("stock.tx.topic"))

                .addProcessor(
                        appProperties.getProperty("stock.processor.name"),
                        stockPerformanceProcessorSupplier,
                        (String)appProperties.get("transaction.source.nodename")
                )
                .addStateStore(storeBuilder, appProperties.getProperty("stock.processor.name"))
                .addSink(
                        (String)appProperties.get("stock.processor.sink"),
                        (String)appProperties.get("stock.performance.sink"),
                        stringSerde.serializer(),
                        stockPerformanceSerializer,
                        appProperties.getProperty("stock.processor.name")
                        )
                .addProcessor("performancePrinter",
                        new KafkaStreamDebugPrinter("performance"),
                        appProperties.getProperty("stock.processor.name")
                        );

        System.out.println(topology.describe().toString());
        return topology;
    }
}
