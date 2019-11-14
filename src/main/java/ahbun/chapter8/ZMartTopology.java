package ahbun.chapter8;

import ahbun.chap3.zeemart.SecurityDBService;
import ahbun.chap4.partitioner.RewardsStreamPartitioner;
import ahbun.chap4.transformer.PurchaseRewardTransformer;
import ahbun.lib.StreamsSerdes;
import ahbun.model.Purchase;
import ahbun.model.PurchasePattern;
import ahbun.model.Reward;
import ahbun.model.RewardAccumulator;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

import static ahbun.lib.ZMartLib.*;
import static ahbun.lib.ZMartLib.employeeIDFilter;

public class ZMartTopology {
    enum STREAM_BRANCH {
        CAFE,
        ELECTRONICS,
    }

    public static Topology getTopology(final Properties properties, final Properties businessProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        StreamsSerdes.PurchaseSerde ps = new StreamsSerdes.PurchaseSerde();
        StreamsSerdes.PurchasePatternSerde pps = new StreamsSerdes.PurchasePatternSerde();
        StreamsSerdes.RewardSerde rss = new StreamsSerdes.RewardSerde();

        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(ps.serializer(), ps.deserializer());

        // Consume from Source
        final KStream<String, Purchase> input = builder.stream(properties.getProperty("in_topic"),
                //  serialize    key     , value
                Consumed.with(stringSerde, purchaseSerde));
        // create mask Purchase stream

        KStream<String, Purchase> ksMaskedStream = input.mapValues(maskCreditCardMapper);


        //masked stream-> purchase pattern stream -> sink
        //                 *
        KStream<String, PurchasePattern> ksPurchasePP = ksMaskedStream.mapValues(purchasePToPatternValueMapper);


        ksPurchasePP.to(properties.getProperty("pattern_topic"), Produced.with(stringSerde, pps));
        // debug print to console: ksPurchasePP.print(Printed.<String, PurchasePattern>toSysOut().withLabel("purchase-pattern"));
        // masked stream -> reward -> sink
        KStream<String, Reward> rewardStream = ksMaskedStream.mapValues(purchaseRewardValueMapper);
        // debug print to console: rewardStream.print(Printed.<String, Reward>toSysOut().withLabel("reward"));
        rewardStream.to(properties.getProperty("reward_topic"),Produced.with(stringSerde, rss));

        // build storage sink
        // Requirement a: filter purchase price and remap key to purchase date
        ksMaskedStream.
                filter(minimumPurchaseFilter)
                .selectKey(reMapKeyToPurchaseDate);
        ksMaskedStream.to(properties.getProperty("out_topic"), Produced.with(stringSerde, purchaseSerde));

        // use branch processor to create branches of topics related to purchases from Cafe and Electronics subsidiaries.
        // create predicate for the branches and use branch to obtain the array of KStream that matches the predicates

        KStream<String, Purchase>[] streams = ksMaskedStream.branch(cafePredicate, electronicsPredicate);
        streams[RefactorZMartKSApp.STREAM_BRANCH.CAFE.ordinal()].to(properties.getProperty("cafe_sink"),
                Produced.with(stringSerde, purchaseSerde));

        streams[RefactorZMartKSApp.STREAM_BRANCH.ELECTRONICS.ordinal()].to(properties.getProperty("electronics_sink"),
                Produced.with(stringSerde, purchaseSerde));

        ForeachAction<String, Purchase> securityAction = (k, p) -> SecurityDBService
                .saveRecord(p.getPurchaseDate(), p.getEmployeeId(), p.getItemPurchased());

        ksMaskedStream.filter(employeeIDFilter).foreach(securityAction);
        return builder.build();
    }

    public static Topology getKeyStoreTopolopgy(final Properties properties, final Properties businessProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        StreamsSerdes.PurchaseSerde ps = new StreamsSerdes.PurchaseSerde();
        StreamsSerdes.PurchasePatternSerde pps = new StreamsSerdes.PurchasePatternSerde();
        StreamsSerdes.RewardSerde rss = new StreamsSerdes.RewardSerde();
        StreamsSerdes.RewardAccumatorSerde rewardAccumatorSerde = new StreamsSerdes.RewardAccumatorSerde();
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(ps.serializer(), ps.deserializer());

        // Consume from Source
        final KStream<String, Purchase> input = builder.stream(properties.getProperty("in_topic"),
                //  serialize    key     , value
                Consumed.with(stringSerde, purchaseSerde));
        // create mask Purchase stream

        KStream<String, Purchase> ksMaskedStream = input.mapValues(maskCreditCardMapper);


        //masked stream-> purchase pattern stream -> sink
        //                 *
        KStream<String, PurchasePattern> ksPurchasePP = ksMaskedStream.mapValues(purchasePToPatternValueMapper);


        ksPurchasePP.to(properties.getProperty("pattern_topic"), Produced.with(stringSerde, pps));


        // add state to compute accumulated reward points
        String rewardsStateStoreName = "rewardPointsStore";
        RewardsStreamPartitioner streamPartitioner = new RewardsStreamPartitioner();
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName);
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier,
                Serdes.String(), Serdes.Integer());
        builder.addStateStore(storeBuilder);

        KStream<String, Purchase> transByCustomerStream = ksMaskedStream.through(properties.getProperty("customer_transactions_topic"),
                Produced.with(stringSerde, purchaseSerde, streamPartitioner));

        KStream<String, RewardAccumulator> statefulRewardAccumulator =
                transByCustomerStream.transformValues(()-> new PurchaseRewardTransformer(rewardsStateStoreName), rewardsStateStoreName);
        statefulRewardAccumulator.print(Printed.<String, RewardAccumulator> toSysOut().withLabel("rewards"));
        statefulRewardAccumulator.to(properties.getProperty("reward_topic"), Produced.with(stringSerde, rewardAccumatorSerde));

        // build storage sink
        // Requirement a: filter purchase price and remap key to purchase date
        KeyValueMapper<String, Purchase, Long> mapper = (key, purchase) -> purchase.getpurchaseDateInEpochSeconds();
        Predicate<String, Purchase> predicate = (k,p)->  p.getPrice() * p.getQuantity() >
                Double.parseDouble(businessProperties.getOrDefault("minimum_purchase", "100.00").toString());

        ksMaskedStream.
                filter(predicate)
                .selectKey(mapper);
        ksMaskedStream.to(properties.getProperty("out_topic"), Produced.with(stringSerde, purchaseSerde));

        // use branch processor to create branches of topics related to purchases from Cafe and Electronics subsidiaries.
        // create predicate for the branches and use branch to obtain the array of KStream that matches the predicates

        Predicate<String, Purchase> cafePredicate = (key, purchase) ->
                purchase.getDepartment().equalsIgnoreCase("cafe");

        Predicate<String, Purchase> electronicsPredicate = (key, purchase) ->
                purchase.getDepartment().equalsIgnoreCase("electronics");

        KStream<String, Purchase>[] streams = ksMaskedStream.branch(cafePredicate, electronicsPredicate);

        streams[STREAM_BRANCH.CAFE.ordinal()].to(properties.getProperty("cafe_sink"),
                Produced.with(stringSerde, purchaseSerde));
        streams[STREAM_BRANCH.ELECTRONICS.ordinal()].to(properties.getProperty("electronics_sink"),
                Produced.with(stringSerde, purchaseSerde));

        //
        Predicate<String, Purchase> securityPredicate = (k,p) ->
                p.getEmployeeId().equalsIgnoreCase("0000");

        ForeachAction<String, Purchase> securityAction = (k,p) -> SecurityDBService
                .saveRecord(p.getPurchaseDate(), p.getEmployeeId(), p.getItemPurchased());

        ksMaskedStream.filter(securityPredicate).foreach(securityAction);
        return builder.build();
    }
}
