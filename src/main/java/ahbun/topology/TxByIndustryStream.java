package ahbun.topology;

import ahbun.lib.ApplicationConfig;
import ahbun.lib.StreamsSerdes;
import ahbun.model.StockTransaction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class TxByIndustryStream implements StreamProcessorPathBuilder<String, StockTransaction> {
    private Logger logger = LoggerFactory.getLogger(TxByIndustryStream.class);
    private String sinkTopic = "default-sink";
    private Serde<String> stringSerde = Serdes.String();
    private Serde<Long> longSerde = Serdes.Long();
    private String kvTableName = "default-tx-shares-by-industry";
    private KeyValueBytesStoreSupplier storeSupplier;

    public TxByIndustryStream(String appConfigFullPath) {
        Properties appProperties = new Properties();
        try {
            appProperties.load(new FileReader(appConfigFullPath));
            kvTableName = appProperties.getProperty(ApplicationConfig.LOCAL_STORE);
            sinkTopic = appProperties.getProperty(ApplicationConfig.SINK_TOPIC);
        } catch (IOException ex) {
            logger.info(ex.getMessage());
            logger.info("using default store name: " + kvTableName);
        }

        storeSupplier = Stores.persistentKeyValueStore(kvTableName);
    }

    @Override
    public void attachStreamPath(KStream<String, StockTransaction> sourceStream) {
        sourceStream
                .map((k, v) -> KeyValue.pair(v.getIndustry(), (long) v.getShares())) // remap key as industry sector
                .groupByKey(Grouped.with(stringSerde, longSerde))
                .aggregate(() -> 0L,
                        (symbol, tx, count) -> count += tx,
                        Materialized.<String, Long>as(storeSupplier).withKeySerde(stringSerde)
                                .withValueSerde(longSerde))
                .toStream()
                .peek((k,v)-> logger.info("peek:" + k + ":" + v.toString()))
                .to(sinkTopic);
    }
}
