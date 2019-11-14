package ahbun.chapter8;

import ahbun.lib.StreamsSerdes;
import ahbun.lib.TrackTop5StockLib;
import ahbun.model.ShareVolume;
import ahbun.model.StockTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;

import java.util.Properties;

public class StockTop5Topology {
    public static Topology getTopology(Properties appProperties, Logger logger) {
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
                .mapValues(TrackTop5StockLib.txToShareVolume)
                .groupBy(TrackTop5StockLib.reMapSymbolAsKey,
                        Grouped.with(stringSerde, shareVolumeSerde))
                .reduce(ShareVolume::sum);
        /**
         * perform a top-5 aggregation on the shareVolumeTotalStream
         * 1. re-group items by industry
         * 2. perform aggregation in a fixed sized queue
         * 3. map the queue to a string for reporting
         */

        // instantiate a fixed-size queue to store the top n results
        //Comparator<ShareVolume> comparator = (sv1, sv2) -> sv2.getVolume() - sv1.getVolume();
        StreamsSerdes.FixedSizePQSerde fixedSizePQSerde = new StreamsSerdes.FixedSizePQSerde();

        // FixedSizePriorityQueue contains top N most active share volume in a industry
        //
        // value mapper collects the industry name, and respective trading symbol and its
        // respective volume from the queue and generates the aggregated information as string.
        //
        // mapper to convert the queue to results ///Grouped
        shareVolumeTotalStream
                .groupBy(TrackTop5StockLib.mapIndustryAsKey,
                        Grouped.with(stringSerde, shareVolumeSerde))
                .aggregate(TrackTop5StockLib.fixedSizePriorityQueueInitializer
                        ,
//                        ()->
//                                new FixedSizePriorityQueue<>(TrackTop5StockLib.comparePQByVolume,
//                                        Integer.parseInt(appProperties.getProperty("top.n"))),
                        (k,v,agg) -> agg.add(v),
                        (k,v,agg) -> agg.remove(v),
                        Materialized.with(stringSerde, fixedSizePQSerde))
                .mapValues(TrackTop5StockLib.mapPQToTop5Summary)
                .toStream().peek((k,v)-> logger.info(v.toString()))
                .to(appProperties.getProperty("topN.by.industry"), Produced.with(stringSerde, stringSerde));

        return builder.build();
    }
}
