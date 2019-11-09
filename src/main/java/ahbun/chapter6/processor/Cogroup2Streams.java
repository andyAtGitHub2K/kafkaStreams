package ahbun.chapter6.processor;

import ahbun.lib.StreamsSerdes;
import ahbun.model.ClickEvent;
import ahbun.model.StockTransaction;
import ahbun.util.KafkaStreamDebugPrinter;
import ahbun.util.MockDataProducer;
import ahbun.util.Tuple;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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
import java.time.Duration;
import java.util.*;

import static ahbun.util.MockDataProducer.*;

/***
 * Cogroup2Streams combines two streams ({@link ahbun.model.ClickEvent} and {@link ahbun.model.StockTransaction})
 * by a common key to form a Tuple.
 */
public class Cogroup2Streams {
    private static Logger logger = LoggerFactory.getLogger(Cogroup2Streams.class);

    public static void main(String[] argv) throws IOException, InterruptedException {
        Properties streamPproperties =  new Properties();
        streamPproperties.load(Objects
                .requireNonNull(
                        ClassLoader
                                .getSystemResourceAsStream("chapter6/cogroup_stream.properties")));

        Properties appProperties = new Properties();
        appProperties.load(Objects
                .requireNonNull(
                        ClassLoader
                                .getSystemResourceAsStream("chapter6/cogroup_app.properties")));
        //createTopology(appProperties);
        KafkaStreams kafkaStreams = new KafkaStreams(createTopology(appProperties), streamPproperties);
        kafkaStreams.cleanUp();


//(int iteration, int max, int customerSize, String clickEventTopic, String txTopic)
        produceTxAndClickEvents(20, 5, 3,
                appProperties.getProperty("click.event.topic"),
                appProperties.getProperty("stock.tx.topic"));
        //Thread.sleep(10000);
        //produceStockTransaction(10, 5,
          //      appProperties.getProperty("stock.tx.topic"),
         //       10, 300, null);
        try {
            kafkaStreams.start();
            Thread.sleep(30000);
        } catch (InterruptedException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        finally {
            kafkaStreams.close();
            MockDataProducer.shutdown();
        }
    }

    private static Topology createTopology(Properties appProperties) {
        Serde<String>  stringSerde = Serdes.String();
        Deserializer<ClickEvent> clickEventDeserializer = StreamsSerdes.ClickEventSerde().deserializer();
        Deserializer<StockTransaction> stockTransactionDeserializer = StreamsSerdes.StockTransactionSerde().deserializer();
        Serde<Tuple<List<ClickEvent>, List<StockTransaction>>> tupleSerde = StreamsSerdes.CTSerceTupleSerde();

        // create local key value store
        Map<String, String> changeLogConfigs = new HashMap<>();
        changeLogConfigs.put("retention.ms", "120000");
        changeLogConfigs.put("cleanup.policy", "compact,delete");

        boolean useInMemory = false;

        String keyStoreName = appProperties.getProperty("kvstore.name");
        if (!useInMemory) {
            keyStoreName = appProperties.getProperty("persist.keystore.name");
        }

        final String processorStoreName = keyStoreName;

        Duration duration = Duration.ofMillis(Long.parseLong(appProperties.getProperty("punctuator.interval.ms")));
        System.out.println("integer: " + Integer.parseInt(appProperties.getProperty("punctuator.interval.ms")));
        System.out.println("duration " + duration.toString());

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(keyStoreName);
        if (!useInMemory) {
             storeSupplier = Stores.persistentKeyValueStore(keyStoreName);
        }

        //
        StoreBuilder<KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>>>
                storeStoreBuilder = Stores.keyValueStoreBuilder(storeSupplier,
                stringSerde, tupleSerde);
        if (!useInMemory) {
            storeStoreBuilder.withLoggingDisabled(); //withLoggingEnabled(changeLogConfigs);
        }

        ProcessorSupplier<String, Tuple<ClickEvent, StockTransaction>> cogroupProcessorSupplier =
                () -> new CogroupProcessor(processorStoreName, duration);

        Topology topology = new Topology();
        topology.addSource(Topology.AutoOffsetReset.LATEST,
                appProperties.getProperty("transaction.source.nodename"),
                stringSerde.deserializer(),
                stockTransactionDeserializer,
                appProperties.getProperty("stock.tx.topic")
                );

        topology.addSource(Topology.AutoOffsetReset.LATEST,
                appProperties.getProperty("click.event.source.nodename"),
                stringSerde.deserializer(),
                clickEventDeserializer,
                appProperties.getProperty("click.event.topic")
        );

        topology.addProcessor(appProperties.getProperty("tx.processor"),
                StockTxToTupleProcessor::new,
                appProperties.getProperty("transaction.source.nodename")
                );

        topology.addProcessor(appProperties.getProperty("click.event.processor"),
                ClickEventToTupleProcessor::new,
                appProperties.getProperty("click.event.source.nodename")
                );



        // add coProcessor to join the two streams
        topology.addProcessor(
                appProperties.getProperty("aggregate.processor"),
                cogroupProcessorSupplier,
                appProperties.getProperty("click.event.processor"),
                appProperties.getProperty("tx.processor")
                )
                .addStateStore(storeStoreBuilder, appProperties.getProperty("aggregate.processor"));

        topology.addSink("cogroupsink",
                appProperties.getProperty("cogroup.sink"),
                appProperties.getProperty("aggregate.processor")
                );
        // debug
       /* topology.addProcessor("tupleTxPrinter",
               new KafkaStreamDebugPrinter("*tx debug*"),
                appProperties.getProperty("tx.processor")
        );

        topology.addProcessor("tupleCkEventPrinter",
                new KafkaStreamDebugPrinter("click debug"),
                appProperties.getProperty("click.event.processor")
                );*/

        topology.addProcessor("co-processor-debug",
                new KafkaStreamDebugPrinter("co-processor debug"),
                appProperties.getProperty("aggregate.processor")
        );
        logger.info(topology.describe().toString());
        return topology;
    }
}
