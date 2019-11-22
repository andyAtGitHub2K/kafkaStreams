package ahbun.topology;

import ahbun.lib.StreamsSerdes;
import ahbun.model.StockTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class TotalTxByMarketSector implements TopologyBuilder {
    private Properties appConfig;

    public TotalTxByMarketSector(String appConfigPath) throws IOException {
        appConfig = new Properties();
        appConfig.load(Objects
                .requireNonNull(
                        ClassLoader
                                .getSystemResourceAsStream(appConfigPath)));
    }

    @Override
    public StreamsBuilder initBuilder(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<Long> longSerde = Serdes.Long();

        // consume from source
        final KStream<String, StockTransaction> inputStream =
                builder.stream(appConfig.getProperty("in.topic"),
                        Consumed.with(stringSerde, stockTransactionSerde));

        inputStream
                .map((k,v) -> KeyValue.pair(v.getIndustry(), v)) // remap key as industry sector
                .groupByKey(Grouped.with(stringSerde, stockTransactionSerde))
                .aggregate(()-> 0L,
                        (symbol, tx, count) -> count = count + tx.getShares(),
                        Materialized.with(stringSerde, longSerde))
                .toStream()
                .to(appConfig.getProperty("out.topic"));

        return builder;
    }

    public String sourceTopic() {
        return appConfig.getProperty("in.topic");
    }

    public String sinkTopic() {
        return appConfig.getProperty("out.topic");
    }
}
