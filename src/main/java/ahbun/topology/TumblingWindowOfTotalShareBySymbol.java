package ahbun.topology;

import ahbun.lib.ApplicationConfig;
import ahbun.lib.StreamsSerdes;
import ahbun.model.StockTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

/***
 * Gather total shares traded in tumbling window
 */
public class TumblingWindowOfTotalShareBySymbol implements StreamProcessorPathBuilder<String, StockTransaction> {
    private Logger logger = LoggerFactory.getLogger(ConsumerSharesPerSession.class);
    private Serde<String> stringSerde = Serdes.String();
    private Serde<Long> longSerde = Serdes.Long();
    private Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
    private KeyValueMapper<String, StockTransaction, String> mapKeyAsSymbol
            = (k, v) -> v.getSymbol();
    private WindowBytesStoreSupplier storeSupplier;
    private WindowedSerdes.TimeWindowedSerde windowedSerde;
    private TimeWindows tw;
    private String sinkTopic;
    private long tumblingWindowSizeInSecond;
    private String kvTableName;
    public TumblingWindowOfTotalShareBySymbol(String appConfigFullPath) throws IOException {
        Properties appConfig = new Properties();
        appConfig.load(new FileReader(appConfigFullPath));
         tumblingWindowSizeInSecond = Long.parseLong(appConfig.getProperty(ApplicationConfig.TUMBLING_WINDOWS_SIZE_SECOND));
        long retentionPeriodInSecond = Long.parseLong(appConfig.getProperty(ApplicationConfig.RETENTION_PERIOD_SECOND));
        long gracePeriodInSecond =  Long.parseLong(appConfig.getProperty(ApplicationConfig.GRACE_PERIOD_SECOND));
        logger.info("tumblingWindowSizeInSecond {}", tumblingWindowSizeInSecond);
        logger.info("retentionPeriodInSecond {}", retentionPeriodInSecond);
        sinkTopic = appConfig.getProperty(ApplicationConfig.SINK_TOPIC);
        kvTableName = appConfig.getProperty(ApplicationConfig.LOCAL_STORE);
        windowedSerde = new WindowedSerdes.TimeWindowedSerde(stringSerde, tumblingWindowSizeInSecond * 1000);
        storeSupplier = Stores.persistentWindowStore(kvTableName, Duration.ofSeconds(retentionPeriodInSecond),
                Duration.ofSeconds(tumblingWindowSizeInSecond), false);

         tw = TimeWindows
                 .of(Duration.ofSeconds(tumblingWindowSizeInSecond))
                 .grace(Duration.ofSeconds(gracePeriodInSecond));
    }

    /*@Override
    public void attachStreamPath(KStream<String, StockTransaction> sourceStream) {
        KeyValueMapper<Windowed<String>, Long, KeyValue<String, Long>> mapper = (key, value) ->
                new KeyValue<>(key.key() + "@" + Instant.ofEpochMilli(key.window().start()).toString() + "->" +
                        Instant.ofEpochMilli(key.window().end()).toString(), value);
        sourceStream
                .groupBy(mapKeyAsSymbol,
                        Grouped.with(stringSerde, stockTransactionSerde))
                .windowedBy(tw)
                .aggregate(() -> 0L,
                        (k,v,aggr) -> aggr += v.getShares(),
                        Materialized.<String, Long> as(storeSupplier)
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde))
                .toStream()
                .to(sinkTopic, Produced.with(windowedSerde, longSerde));
    }*/

    @Override
    public void attachStreamPath(KStream<String, StockTransaction> sourceStream) {
        KeyValueMapper<Windowed<String>, Long, KeyValue<String, Long>> mapper = (key, value) ->
                new KeyValue<>(key.key() + "@" + Instant.ofEpochMilli(key.window().start()).toString() + "->" +
                        Instant.ofEpochMilli(key.window().end()).toString(), value);

        KStream<Windowed<String>, Long> ks =
                sourceStream
                        .groupBy(mapKeyAsSymbol,
                                Grouped.with(stringSerde, stockTransactionSerde))
                        .windowedBy(TimeWindows
                                .of(Duration.ofSeconds(tumblingWindowSizeInSecond)))
                        .aggregate(() -> 0L,
                                (k, v, aggr) -> aggr += v.getShares(),
                                Materialized.with(stringSerde, longSerde))
                        .toStream();


        ks.groupByKey().reduce((value1, value2) -> value2, Materialized.as(kvTableName));

        ks.map(mapper)
                .to(sinkTopic, Produced.with(stringSerde, longSerde));
    }
}
