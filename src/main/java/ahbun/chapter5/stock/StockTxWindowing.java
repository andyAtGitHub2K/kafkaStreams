package ahbun.chapter5.stock;

import ahbun.lib.StreamsSerdes;
import ahbun.model.StockTransaction;
import ahbun.model.TransactionSummary;
import ahbun.util.MockDataProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;


import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;

/***
 * StockTxWindowing demonstrates the use of session windows to track traders' activities over
 * the defined session period for reasons such as analyzing trading strategies.
 *
 * The pipeline begins with
 *    - consuming trading transactions
 *       - group the tx by customer id and stock symbol in TransactionSummary
 *         - count number of transactions by TransactionSummary using
 *                (session, tumbling  or hopping) window.
 *           - write out the results to the console or topic
 *
 *
 *  - define abstraction for TransactionSummary
 *  - create builder, serde
 *  - build pipeline
 */
public class StockTxWindowing {
    public static void main(String[] args) throws IOException, InterruptedException {
        // initialize the properties for Kafka stream and topics
        Properties streamProperties = new Properties();
        streamProperties.load(
                Objects.requireNonNull(
                        ClassLoader
                        .getSystemResourceAsStream("chapter5/window/kafkaStreamConfig.properties")
                ));
        Properties appProperties = new Properties();
        appProperties.load(
                Objects.requireNonNull(
                        ClassLoader
                                .getSystemResourceAsStream("chapter5/window/kafkaAppConfig.properties")
                )
        );

        // create topology
        // build stream using the topology

        // Session Window
        //KafkaStreams kafkaStreams = new KafkaStreams(getSessionWindowTopology(appProperties), streamProperties);

        // Tumbling Window
        //KafkaStreams kafkaStreams = new KafkaStreams(getSTumblingWindowTopology(appProperties), streamProperties);

        // Sliding Window
        KafkaStreams kafkaStreams = new KafkaStreams(getSlidingWindowTopology(appProperties), streamProperties);

        // start data generator
        int[] windowSizeInSecond = new int[]{25, 10, 15};
        int[] windowGap = new int[]{5, 30, 20};
        String[] industryList = {"food", "medical", "wine", "car"};
        kafkaStreams.cleanUp();
        Thread.sleep(4000);
        MockDataProducer.produceTxWithinWindows(appProperties.getProperty("transaction.topic"),
                3,4,2,4,
                windowSizeInSecond, windowGap,
                appProperties.getProperty("financial.news.topic"), industryList);
        // start stream
        kafkaStreams.start();

        Thread.sleep(25000);

        // stop stream
        kafkaStreams.close();

        // stop data generator
        MockDataProducer.shutdown();
    }

    private static Topology getSessionWindowTopology(Properties appProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        Serde<StockTransaction> transactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<TransactionSummary> transactionSummarySerde =  StreamsSerdes.TxSummarySerde();
        Serde<String> stringSerde = Serdes.String();

        SessionWindowedKStream<TransactionSummary, StockTransaction> customerTransactionCounts =
                builder.stream(appProperties.getProperty("transaction.topic"),
                        Consumed
                                .with(stringSerde, transactionSerde)
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .groupBy((noKey, tx) ->
                    TransactionSummary.builder(tx).build(),
                            Grouped.with(transactionSummarySerde, transactionSerde))
                .windowedBy(SessionWindows
                        .with(Duration.ofMillis(
                        Integer.parseInt(appProperties.getProperty("inactive.gap.milli"))))
                        .grace(Duration.ofSeconds(1)));

        customerTransactionCounts
                .count(Materialized.<TransactionSummary, Long, SessionStore<Bytes, byte[]>>as(
                        appProperties.getProperty("key.store.name"))
                        .withKeySerde(transactionSummarySerde)
                        .withValueSerde(Serdes.Long()).withRetention(Duration.ofMinutes(1))
                )
                .toStream()
                .map((key,value)-> new KeyValue<>(key.key() + "@\n" + Instant.ofEpochMilli(key.window().start()).toString() + "->" +
                        Instant.ofEpochMilli(key.window().end()).toString() + "\n", value))
             .print(Printed.<String, Long>toSysOut()
                        .withLabel(appProperties.getProperty("print.label")));
        return builder.build();
    }

    private static Topology getSTumblingWindowTopology(Properties appProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        Serde<StockTransaction> transactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<TransactionSummary> transactionSummarySerde =  StreamsSerdes.TxSummarySerde();
        Serde<String> stringSerde = Serdes.String();

        TimeWindowedKStream<TransactionSummary, StockTransaction> customerTransactionCounts =
                builder.stream(appProperties.getProperty("transaction.topic"),
                        Consumed
                                .with(stringSerde, transactionSerde)
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                        .groupBy((noKey, tx) ->
                                        TransactionSummary.builder(tx).build(),
                                Grouped.with(transactionSummarySerde, transactionSerde))
                        .windowedBy(TimeWindows
                                .of(Duration.ofSeconds(
                                        Integer.parseInt(appProperties.getProperty("tumbling.window.period.second")))));

        customerTransactionCounts
                .count()
                .toStream()
                .map((key,value)-> new KeyValue<>(key.key() + "@\n" + Instant.ofEpochMilli(key.window().start()).toString() + "->" +
                        Instant.ofEpochMilli(key.window().end()).toString() + "\n", value))
                .print(Printed.<String, Long>toSysOut()
                        .withLabel(appProperties.getProperty("print.label")));
        return builder.build();
    }

    private static Topology getSlidingWindowTopology(Properties appProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        Serde<StockTransaction> transactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<TransactionSummary> transactionSummarySerde =  StreamsSerdes.TxSummarySerde();
        Serde<String> stringSerde = Serdes.String();

        TimeWindowedKStream<TransactionSummary, StockTransaction> customerTransactionCounts =
                builder.stream(appProperties.getProperty("transaction.topic"),
                        Consumed
                                .with(stringSerde, transactionSerde)
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                        .groupBy((noKey, tx) ->
                                        TransactionSummary.builder(tx).build(),
                                Grouped.with(transactionSummarySerde, transactionSerde))
                        .windowedBy(TimeWindows
                                .of(Duration.ofSeconds(
                                        Integer.parseInt(appProperties.getProperty("tumbling.window.period.second"))))
                                .advanceBy(Duration.ofSeconds(
                                        Integer.parseInt(appProperties.getProperty("advance.window.second"))
                                ))
                        );

        customerTransactionCounts
                .count()
                .toStream()
                .map((key,value)-> new KeyValue<>(key.key() + "@\n" + Instant.ofEpochMilli(key.window().start()).toString() + "->" +
                        Instant.ofEpochMilli(key.window().end()).toString() + "\n", value))
                .print(Printed.<String, Long>toSysOut()
                        .withLabel(appProperties.getProperty("print.label")));
        return builder.build();
    }

}
