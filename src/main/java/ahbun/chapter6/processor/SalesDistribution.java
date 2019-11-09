package ahbun.chapter6.processor;

import ahbun.lib.JsonDeserializer;
import ahbun.lib.JsonSerializer;
import ahbun.model.BeerDistribution;
import ahbun.util.KafkaStreamDebugPrinter;
import ahbun.util.MockDataProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.UsePreviousTimeOnInvalidTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

/***
 * Using Processor API to build Stream Topology
 *
 * 1. consume sales and separate domestic and international sales to two different topics
 *
 *    models: Beer Purchase - Currency, totalSales, numberOfCases, beerType
 */
public class SalesDistribution {
    private static Logger logger = LoggerFactory.getLogger(SalesDistribution.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        // 1. initialize kafka stream properties
        Properties streamProperties = new Properties();
        streamProperties.load(Objects
                .requireNonNull(
                        ClassLoader
                        .getSystemResourceAsStream("chapter6/kafka_beer_distribution_stream.properties")
                )
        );

        Properties appProperties = new Properties();
        appProperties.load(Objects
                .requireNonNull(
                        ClassLoader
                                .getSystemResourceAsStream("chapter6/beer_distribution_app.properties")
                )
        );

        // 2. create KafkaStream with topology
        KafkaStreams kafkaStreams = new KafkaStreams(buildTopology(appProperties), streamProperties);
        kafkaStreams.cleanUp();

        // 3. start data generator
        logger.info("topic: " + appProperties.getProperty("beer.distribution.topic")); //beer.distribution.topic
        MockDataProducer.produceBeerDistributionMessages(appProperties.getProperty("beer.distribution.topic"), 5);

        // 4. start kafka pipeline
        kafkaStreams.start();

        Thread.sleep(10000);
        // 5. shutdown
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }

    private static Topology buildTopology(Properties appProperties) {
        Serde<String> stringSerde = Serdes.String();
        Deserializer<BeerDistribution> beerDistributionDeserializer = new JsonDeserializer<>(BeerDistribution.class);
        Serializer<BeerDistribution> beerDistributionSerializer = new JsonSerializer<>();
        Topology topology = new Topology();

        // create processor
        BeerDistributionProcessor beerDistributionProcessor =
                new BeerDistributionProcessor(
                        // node names
                        appProperties.getProperty("international.sink.name"),
                        appProperties.getProperty("domestic.sink.name"));

        // create source node
        topology.addSource(
                Topology.AutoOffsetReset.LATEST,
                appProperties.getProperty("purchase.source.nodename"), // a node name for adding child node(s).
                // https://docs.confluent.io/current/installation/configuration/topic-configs.html
                // allow to select event time or ingested time
                // depending on the setting of message.timestamp.type
                // CreateTime or LogAppendTime
                // default is 'CreateTime' (event time)
                new UsePreviousTimeOnInvalidTimestamp(),
                stringSerde.deserializer(), // key
                beerDistributionDeserializer, // value
               // "beer-stream-topic")
                appProperties.getProperty("beer.distribution.topic")) // source topic

                .addProcessor(
                     appProperties.getProperty("beer.currency.processor.name"), // processor node name
                () -> beerDistributionProcessor,
                // name of parent node
                appProperties.getProperty("purchase.source.nodename"));
        // debug
        /* topology.addProcessor("I",//appProperties.getProperty("international.sink.name"),
            //    new KafkaStreamDebugPrinter("----international-sales"),"Processor-node");
              //  appProperties.getProperty("beer.currency.processor.name"));
        //topology.addProcessor("D",//appProperties.getProperty("domestic.sink.name"),
          //      new KafkaStreamDebugPrinter("+++++domestic-sales"),"Processor-node");
            //    appProperties.getProperty("beer.currency.processor.name"));
        // add domestic distribution sink ; */
        topology.addSink(
                appProperties.getProperty("domestic.sink.name"), // unique name of the sink
                appProperties.getProperty("sales.domestic.sink"), // sink topic name
                stringSerde.serializer(), // key serializer
                beerDistributionSerializer, // value serializer
                appProperties.getProperty("beer.currency.processor.name") // parent node name
                );

        // add international distribution sink
        topology.addSink(
                appProperties.getProperty("international.sink.name"), // unique name of the sink
                appProperties.getProperty("sales.international.sink"), // sink topic name
                stringSerde.serializer(), // key serializer
                beerDistributionSerializer, // value serializer
                appProperties.getProperty("beer.currency.processor.name") // parent node name
        );
        logger.info(topology.describe().toString());
        return topology;
    }
}
