package ahbun.chapter5.stock;

import ahbun.lib.FixedSizePriorityQueue;
import ahbun.lib.StreamsSerdes;
import ahbun.model.ShareVolume;
import ahbun.model.StockTransaction;
import ahbun.util.MockDataProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.*;


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
public class TrackTop5HighVolume {
    private static Logger logger = LoggerFactory.getLogger(TrackTop5HighVolume.class);

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
        KafkaStreams kafkaStreams = new KafkaStreams(getTopology(appProperties), streamPproperties);
        // 3. start data generator
        MockDataProducer.produceStockTransaction(4,
                "share-volume-stream", 4,
                200, null);
        // 4. start kafka pipeline
        kafkaStreams.start();
        Thread.sleep(12000);
        //kafkaStreams.close(Duration.of(30, SECONDS));
        kafkaStreams.close();
        logger.info(">>>> pipeline closed");
        MockDataProducer.shutdown();
        logger.info(">>>> shut down");
    }

    private static Topology getTopology(Properties appProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        Serde<StockTransaction> stockTransactionSerde =  StreamsSerdes.StockTransactionSerde();
        Serde<ShareVolume> shareVolumeSerde =  StreamsSerdes.ShareVolumeSerde();
        Serde<String> stringSerde = Serdes.String();

        /**
         * Steps 1 - 4
         * consume the source topic with the serde of K (string) ,V (StockTransaction)
         * map the values of StockTransaction to ShareVolume
         * group the values by symbol
         * reduce to the total share volume
         *   - stored in KTable <String, ShareVolume>
         */
        KTable<String, ShareVolume> shareVolumeTotalStream = builder.stream(
                appProperties.getProperty("share.volume.topic"),
                Consumed
                        .with(stringSerde, stockTransactionSerde)
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .mapValues(tx -> ShareVolume.builder(tx).build())
                .groupBy((k, v) -> v.getSymbol(),
                        Grouped.with(stringSerde, shareVolumeSerde))
                .reduce(ShareVolume::sum);
        /**
         * perform a top-5 aggregation on the shareVolumeTotalStream
         * 1. re-group items by industry
         * 2. perform aggregation in a fixed sized queue
         * 3. map the queue to a string for reporting
         */

        // instantiate a fixed-size queue to store the top n results
        Comparator<ShareVolume> comparator = (sv1, sv2) -> sv2.getVolume() - sv1.getVolume();
        StreamsSerdes.FixedSizePQSerde fixedSizePQSerde = new StreamsSerdes.FixedSizePQSerde();
        NumberFormat numberFormat = NumberFormat.getInstance();

        // FixedSizePriorityQueue contains top N most active share volume in a industry
        //
        // value mapper collects the industry name, and respective trading symbol and its
        // respective volume from the queue and generates the aggregated information as string.
        //
        ValueMapper<FixedSizePriorityQueue, String> valueMapper = fpq -> {
            StringBuilder stringBuilderbuilder = new StringBuilder();
            Iterator<ShareVolume> iterator = fpq.iterator();
            int counter = 1;
            boolean isIndustrySet = false;
            while (iterator.hasNext()) {
                ShareVolume stockVolume = iterator.next();
                if (stockVolume != null) {
                    if (!isIndustrySet) {
                        stringBuilderbuilder.append("[" + stockVolume.getIndustry() + "]: ");
                        isIndustrySet = true;
                    }
                    stringBuilderbuilder.append(counter++).append(")").append(stockVolume.getSymbol())
                            .append(":").append(numberFormat.format(stockVolume.getVolume())).append(" ");
                }
            }
            return stringBuilderbuilder.toString();
        };
        // mapper to convert the queue to results ///Grouped
       shareVolumeTotalStream
                .groupBy((k, v) -> KeyValue.pair(v.getIndustry(), v),
                Grouped.with(stringSerde, shareVolumeSerde))
                .aggregate(()->
                                new FixedSizePriorityQueue<>(comparator,
                                        Integer.parseInt(appProperties.getProperty("top.n"))),
                        (k,v,agg) -> agg.add(v),
                        (k,v,agg) -> agg.remove(v),
                        Materialized.with(stringSerde, fixedSizePQSerde))
               .mapValues(valueMapper)
               .toStream().peek((k,v)-> logger.info(v.toString()))
               .to(appProperties.getProperty("topN.by.industry"), Produced.with(stringSerde, stringSerde));

        return builder.build();
    }
}
