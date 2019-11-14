package ahbun.chapter8;

import ahbun.lib.StreamsSerdes;
import ahbun.lib.TrackTop5StockLib;
import ahbun.model.ShareVolume;
import ahbun.model.StockTransaction;
import ahbun.util.MockDataProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;


/***
 * TrackTop5HighVolume demonstrates the use of aggregation and grouping to gather
 * information meeting specific criterion.
 *
 * In this example, the goal is to obtain
 * the top 5 highest trade volume of selected industries from the stream of
 * stock transactions.
 *
 * 1. create source model and the result data model
 *          StockTransaction -
 *                        symbol, sector, industry, shares
 *                        share prices, customerId,
 *                        transactionTimeStamp, purchase or sell
 *          ShareVolume - symbol, share, industry
 * 2. map the StockTransaction to the data object (share volume)
 *
 * 3. group the share volume by ticker symbol then reduce it to the total volume
 *
 * 4. store the final result into KTable<String, ShareVolume>
 *
 *
 *
 */
public class RefactorTrackTop5HighVolume {
    private static Logger logger = LoggerFactory.getLogger(RefactorTrackTop5HighVolume.class);

    /**
     * Create pipeline topology and start processing the message to obtain
     * the top 5 highest volume industries from
     * trading transactions generated from the data generator
     *
     * @param argv
     */
    public static void main(String[] argv) throws IOException, InterruptedException {
        // 1. initialize kafka stream properties top5tradeVolume.properties
        Properties streamPproperties =  new Properties();
        streamPproperties.load(Objects
                .requireNonNull(
                        ClassLoader
                                .getSystemResourceAsStream("chapter5/kafkaTop5tradeVolumeStreamConfig.properties")));

        Properties appProperties = new Properties();
        appProperties.load(Objects
                .requireNonNull(
                        ClassLoader
                                .getSystemResourceAsStream("chapter5/kafkaTopVolumeAppConfig.properties")));

        // 2. create topology
        KafkaStreams kafkaStreams =
                new KafkaStreams(StockTop5Topology.getTopology(appProperties, logger), streamPproperties);
        // 3. start data generator
        MockDataProducer.produceStockTransaction(4, 5,
                "share-volume-stream", 4,
                200, null);
        // 4. start kafka pipeline
        kafkaStreams.start();
        Thread.sleep(12000);
        kafkaStreams.close();
        logger.info(">>>> pipeline closed");
        MockDataProducer.shutdown();
        logger.info(">>>> shut down");
    }
}
