package ahbun.chap3.zeemart;

import ahbun.lib.StreamsSerdes;
import ahbun.model.Purchase;
import ahbun.model.PurchasePattern;
import ahbun.model.Reward;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/***
 * ZMartKSApp is a Kafka Streaming application that feed enhanced data inputs
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
 */
public class ZMartKSApp {
    enum STREAM_BRANCH {
        CAFE,
        ELECTRONICS
    }

    public static void main(String[] argv) throws IOException, InterruptedException {
        // Load properties
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("chapter3/zeemart.properties");
        Properties properties = new Properties();
        properties.load(inputStream);

        InputStream businessInputStream = classLoader.getResourceAsStream("chapter3/business.properties");
        Properties businessProperties = new Properties();
        businessProperties.load(businessInputStream);

        KafkaStreams kafkaStreams = new KafkaStreams(getTopology(properties, businessProperties), properties);
        kafkaStreams.start();
        Thread.sleep(60000);
        kafkaStreams.close();
    }

    private static Topology getTopology(final Properties properties, final Properties businessProperties) {
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
        KStream<String, Purchase> ksMasked = input.mapValues(p-> Purchase.builder(p).maskCreditCard().build());


        //masked stream-> purchase pattern stream -> sink
        //                 *
        KStream<String, PurchasePattern> ksPurchasePP = ksMasked.mapValues(p ->  PurchasePattern.init(p).build());


        ksPurchasePP.to(properties.getProperty("pattern_topic"), Produced.with(stringSerde, pps));
        // debug print to console: ksPurchasePP.print(Printed.<String, PurchasePattern>toSysOut().withLabel("purchase-pattern"));
        // masked stream -> reward -> sink
        KStream<String, Reward> rewardStream = ksMasked.mapValues(p-> Reward.builder(p).build());
        // debug print to console: rewardStream.print(Printed.<String, Reward>toSysOut().withLabel("reward"));
        rewardStream.to(properties.getProperty("reward_topic"),Produced.with(stringSerde, rss));

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

        Predicate<String, Purchase> cafePredicate = (key, purchase) ->
                purchase.getDepartment().equalsIgnoreCase("cafe");

        Predicate<String, Purchase> electronicsPredicate = (key, purchase) ->
                purchase.getDepartment().equalsIgnoreCase("electronics");

        KStream<String, Purchase>[] streams = ksMasked.branch(cafePredicate, electronicsPredicate);
        streams[STREAM_BRANCH.CAFE.ordinal()].to(properties.getProperty("cafe_sink"),
                Produced.with(stringSerde, purchaseSerde));

        streams[STREAM_BRANCH.ELECTRONICS.ordinal()].to(properties.getProperty("electronics_sink"),
                Produced.with(stringSerde, purchaseSerde));

        //
        Predicate<String, Purchase> securityPredicate = (k,p) ->
                p.getEmployeeId().equalsIgnoreCase("0000");

        ForeachAction<String, Purchase> securityAction = (k,p) -> SecurityDBService
                .saveRecord(p.getPurchaseDate(), p.getEmployeeId(), p.getItemPurchased());

        ksMasked.filter(securityPredicate).foreach(securityAction);
        return builder.build();
    }
}
