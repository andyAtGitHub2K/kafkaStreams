package ahbun.chap4.zeemart;

import ahbun.chap3.zeemart.SecurityDBService;
import ahbun.chap4.partitioner.RewardsStreamPartitioner;
import ahbun.chap4.transformer.PurchaseRewardTransformer;
import ahbun.lib.StreamsSerdes;
import ahbun.lib.TransactionTimestampExtractor;
import ahbun.model.*;
import ahbun.util.MockDataProducer;
import ahbun.util.PurchaseSimiluationRecipe;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

/***
 * ZMartKSAppJoin is a Kafka Streaming application that feed enhanced data inputs
 * to 3 independent services:
 *    1. Purchasing - track all customer purchase transaction
 *    2. Reward - extract customer id and amount spent to determine reward
 *    3. Purchase Pattern - extracts items purchased and zip code to determine
 *                          purchase pattern.
 *
 *  New Requirements:
 *  a. store purchase event only if the purchase price is above minimum price threshold.
 *     use filter and KeyValueMapper to map the KV pair to a new Key associated with the
 *     purchased date
 *  b. branch purchases from new subsidiaries (Cafe, Electronics) to its own topics
 *  c. add branch for security reason to obtain purchase information performed by
 *     a specific employee id.
 *     This requires a filter and a consumer action (ForEachAction)
 *
 *  d. change stateless app to stateful using transformValues to update
 *     accumulated purchase to compute total bonus points.
 *     in each purchase, the app calculates the total bonus points accumulated and
 *     the time interval between the last purchase.
 *
 *  e. join 2 streams to analyze events of the same key over a event window
 *     in terms of the event timestamp
 *
 */
public class ZMartKSAppJoin {
    enum STREAM_BRANCH {
        BEER,
        BOOK,
    }
    private static Logger logger = LoggerFactory.getLogger(ZMartKSAppJoin.class);

    public static void main(String[] argv) throws IOException, InterruptedException {
        try {
            // Load properties
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream inputStream = classLoader.getResourceAsStream("chapter4/zeemart.properties");
            Properties properties = new Properties();
            properties.load(inputStream);

            InputStream businessInputStream = classLoader.getResourceAsStream("chapter4/business.properties");
            Properties businessProperties = new Properties();
            businessProperties.load(businessInputStream);

            // create recipe to mock purchase data
            PurchaseSimiluationRecipe recipe = new PurchaseSimiluationRecipe(5, 3, 10);
            MockDataProducer.producePurchaseData(recipe, properties.getProperty("in_topic"), 500);

            KafkaStreams kafkaStreams = new KafkaStreams(getTopology(properties, businessProperties), properties);
            logger.info("clean up kafka streams");
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            logger.info("kafka streams started");
            Thread.sleep(20000);
            kafkaStreams.close();
            logger.info("shutdown data producer");
        }
        finally {
            MockDataProducer.shutdown();
        }
    }

    private static Topology getTopology(final Properties properties, final Properties businessProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        StreamsSerdes.PurchaseSerde ps = new StreamsSerdes.PurchaseSerde();
        StreamsSerdes.PurchasePatternSerde pps = new StreamsSerdes.PurchasePatternSerde();
        StreamsSerdes.RewardSerde rss = new StreamsSerdes.RewardSerde();
        StreamsSerdes.RewardAccumatorSerde rewardAccumatorSerde = new StreamsSerdes.RewardAccumatorSerde();

        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(ps.serializer(), ps.deserializer());
        String[] topics = {properties.getProperty("beer_sink"), properties.getProperty("book_sink")};
        // Consume from Source
        final KStream<String, Purchase> input = builder.stream(properties.getProperty("in_topic"),
                //  serialize    key     , value
                // using the timestamp of the purchase event
                Consumed
                        .with(stringSerde, purchaseSerde)
                        .withTimestampExtractor(new TransactionTimestampExtractor()));
        // create mask Purchase stream
        KStream<String, Purchase> ksMasked = input.mapValues(p-> Purchase.builder(p).maskCreditCard().build());


        //masked stream-> purchase pattern stream -> sink
        //                 *
        KStream<String, PurchasePattern> ksPurchasePP = ksMasked.mapValues(p ->  PurchasePattern.init(p).build());


        ksPurchasePP.to(properties.getProperty("pattern_topic"), Produced.with(stringSerde, pps));
        // debug print to console: ksPurchasePP.print(Printed.<String, PurchasePattern>toSysOut().withLabel("purchase-pattern"));

        // add state to compute accumulated reward points
        String rewardsStateStoreName = "rewardPointsStore";
        RewardsStreamPartitioner streamPartitioner = new RewardsStreamPartitioner();
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName);
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier,
                Serdes.String(), Serdes.Integer());
        builder.addStateStore(storeBuilder);

        KStream<String, Purchase> transByCustomerStream = ksMasked.through(properties.getProperty("customer_transactions_topic"),
                Produced.with(stringSerde, purchaseSerde, streamPartitioner));

        KStream<String, RewardAccumulator> statefulRewardAccumulator =
                transByCustomerStream.transformValues(()-> new PurchaseRewardTransformer(rewardsStateStoreName), rewardsStateStoreName);
        statefulRewardAccumulator.print(Printed.<String, RewardAccumulator> toSysOut().withLabel("rewards"));
        statefulRewardAccumulator.to(properties.getProperty("reward_sink"), Produced.with(stringSerde, rewardAccumatorSerde));

        // build storage sink
        // Requirement a: filter purchase price and remap key to purchase date
        KeyValueMapper<String, Purchase, Long> mapper = (key, purchase) -> purchase.getpurchaseDateInEpochSeconds();
        Predicate<String, Purchase> predicate = (k,p)->  p.getPrice() * p.getQuantity() >
                Double.parseDouble(businessProperties.getOrDefault("minimum_purchase", "100.00").toString());

        ksMasked.
                filter(predicate)
                .selectKey(mapper);
        ksMasked.to(properties.getProperty("out_topic"), Produced.with(stringSerde, purchaseSerde));

        // use branch processor to create branches of topics related to purchases from Cafe and Electronics subsidiaries.
        // create predicate for the branches and use branch to obtain the array of KStream that matches the predicates

        Predicate<String, Purchase> beerPredicate = (key, purchase) ->
                purchase.getDepartment().equalsIgnoreCase("beer");

        Predicate<String, Purchase> bookPredicate = (key, purchase) ->
                purchase.getDepartment().equalsIgnoreCase("book");

        KStream<String, Purchase>[] streams = ksMasked
                .selectKey((k,v) -> v.getCustomerId())
                .branch(beerPredicate, bookPredicate);
        //STREAM_BRANCH.BEER.ordinal() properties.getProperty("beer_sink")

        KStream<String, Purchase> beerStream = streams[STREAM_BRANCH.BEER.ordinal()];
        KStream<String, Purchase> bookStream = streams[STREAM_BRANCH.BOOK.ordinal()];
        beerStream.to("beer-out",
                Produced.with(stringSerde, purchaseSerde));
        //STREAM_BRANCH.BOOK.ordinal() properties.getProperty("book_sink")
        bookStream.to("book-out",
                Produced.with(stringSerde, purchaseSerde));

        // do join - create a ValueJoiner, JoinWindow, then join the two streams
        ValueJoiner<Purchase, Purchase, CorrelatedPurchase> purchaseJoiner =
                new PurchaseJoiner();
        JoinWindows twentyMinutesWindow = JoinWindows.of(Duration.ofMinutes(20));
        KStream<String, CorrelatedPurchase> joinedStream =
                beerStream.join(bookStream,
                        purchaseJoiner,
                        twentyMinutesWindow,
                        Joined.with(stringSerde, purchaseSerde, purchaseSerde));

        joinedStream.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("joinedStream"));
        //
        Predicate<String, Purchase> securityPredicate = (k,p) ->
                p.getEmployeeId().equalsIgnoreCase("0000");

        ForeachAction<String, Purchase> securityAction = (k,p) -> SecurityDBService
                .saveRecord(p.getPurchaseDate(), p.getEmployeeId(), p.getItemPurchased());

        ksMasked.filter(securityPredicate).foreach(securityAction);
        return builder.build();
    }
}
