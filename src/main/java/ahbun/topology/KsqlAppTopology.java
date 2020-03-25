package ahbun.topology;

import ahbun.lib.StreamsSerdes;
import ahbun.model.StockTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class KsqlAppTopology implements TopologyBuilder<String, StockTransaction> {
    private Properties appConfig;
    private Serde<String> stringSerde = Serdes.String();
    private Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
    private StreamsBuilder streamsBuilder;
    private KStream<String, StockTransaction> sourceStream;

    public KsqlAppTopology(String appConfigPath) throws IOException {
        appConfig = new Properties();
        appConfig.load(Objects
                .requireNonNull(
                        ClassLoader
                                .getSystemResourceAsStream(appConfigPath)));

        initBuilder();
    }

    @Override
    public void initBuilder() {
        // consume from source
        streamsBuilder = new StreamsBuilder();
        sourceStream =
                streamsBuilder.stream(appConfig.getProperty("in.topic"),
                        Consumed.with(stringSerde, stockTransactionSerde));
    }

    @Override
    public Topology build() {
        return streamsBuilder.build();
    }

    @Override
    public void attachProcessorStream(StreamProcessorPathBuilder<String, StockTransaction> streamProcessorPathBuilder) {
        streamProcessorPathBuilder.attachStreamPath(sourceStream);
    }

    public String sourceTopic() {
        return appConfig.getProperty("in.topic");
    }

    public String sinkTopic() {
        return appConfig.getProperty("out.topic");
    }
}
