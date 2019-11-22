package ahbun.chapter9;

import ahbun.lib.StreamsSerdes;
import ahbun.model.StockTransaction;
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
import java.util.concurrent.CountDownLatch;

/***
 * KafkaConnect illustrates the process of using Kafka Connect to adapt legacy database backend to
 * a Kafka Streams application.
 *
 * Objectives: To show the most recent transactions and related information in real time.
 *
 * Approach: Use Kafka Connect source to a database (relational in this example) and stream the update
 * to a Kafka topic.
 *
 * Backend - zookeeper, kafka, h2 database engine
 *
 * Preparation:
 *           download h2 database engine jar from http://www.h2database.com
 *           and create database http://www.h2database.com/html/tutorial.html#creating_new_databases ( java -cp h2-*.jar org.h2.tools.Shell)
 *           http://packages.confluent.io/maven/io/confluent/kafka-connect-jdbc/5.3.0/kafka-connect-jdbc-5.3.0.jar
 *
 * Steps to run the application:
 *          1. start zookeeper, kafka
 *          2. create database [java -cp kafka-connect-jdbc-h2.jar org.h2.tools.Shell]
 *          3. start dbserver and insert data using client
 *             ./gradlew runDBServer
 *             ./gradlew runDBInsertClient
 *
 *          4. start Kafka connect standalone to write records to topic
 *             $KAFKA/bin/connect-standalone.sh  $KAFKA/config/ksinaction/connect-standalone.properties $KAFKA/config/ksinaction/connector-jdbc.properties
 *
 *           5. start Kafka Import DB app
 *             ./gradlew runKafkaDBImport
 *
 *
 * Preparation:
 *    download JDK 11, Elasticsearch/Kibana/x-pack-sql-jdbc-7.4.2.jar 7.4.2,
 *    http://packages.confluent.io/maven/io/confluent/kafka-connect-jdbc/5.3.0/kafka-connect-jdbc-5.3.0.jar
 *    update kibana.yml per instruction
 *
 * Another type of application is to have Kafka Connect to push data to external db or files from
 * a Kafka cluster.
 *
 *  Using Connector
 *     https://docs.confluent.io/current/connect/userguide.html#connect-userguide-standalone-config
 *
 *     bin/connect-standalone worker.properties connector1.properties
 *
 *     worker.properties
 *         Kafka cluster properties and serialization/deserialization
 *         https://docs.confluent.io/current/connect/references/allconfigs.html#connect-allconfigs  worker config
 *
 *     connector1.properties
 *        source connection and transformation properties for connector to move records from data source
 *        to Kafka topic.
 *
 * References:
 *  https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html
 *
 * Prepare H2 database for client to create table through which Kafka Connect used to generate message to
 * a topic for further processing:
 *
 *   create db using Shell, user = sa , password = ENTER
 *
 */
public class KafkaImportNewRecordsFromDB {
    private static Logger logger = LoggerFactory.getLogger(KafkaImportNewRecordsFromDB.class);
    private final static String STREAM_CONFIG = "chapter9/conf/kStreamAggregateTxFromDB.properties";
    private final static String APP_CONFIG = "chapter9/conf/aggregationApp.properties";

    public static void main(String[] argv) throws IOException {
        Properties streamProperties = new Properties();
        streamProperties.load(
                Objects.requireNonNull(ClassLoader.getSystemResourceAsStream(STREAM_CONFIG)));
        Properties appProperties = new Properties();
        appProperties.load(
                Objects.requireNonNull(ClassLoader.getSystemResourceAsStream(APP_CONFIG)));
        KafkaStreams kafkaStreams = new KafkaStreams(Objects
                .requireNonNull(getTopology(appProperties)), streamProperties);
        CountDownLatch doneSignal = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            doneSignal.countDown();
            logger.info("Shutting down the Stock Analysis KStream Connect App Started now");
            kafkaStreams.close();
        }));

        try {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            logger.info("kafka connect app started ....");
            doneSignal.await();
            logger.info("kafka connect app finished");
        } catch (InterruptedException ex) {
            logger.error(ex.getMessage());
        }
    }

    /***
     * Kafka Connect write the message to a topic formed by the prefix + table name.
     * Prefix is set in the chapter9/conf/connector-jdbc.properties while table name
     * is defined in the create table statement of {@link H2DbClient}
     *
     * @param appProperties
     * @return
     */
    private static Topology getTopology(Properties appProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        Serde<StockTransaction> sts = StreamsSerdes.StockTransactionSerde();
        Serde<String> serdes =  Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KStream<String, StockTransaction> txStream = builder.stream(appProperties.getProperty("in.topic"),
                Consumed.with(serdes, sts));
        txStream.peek((k,v) ->
                logger.info("tx - key {}, value {}", k, v))
                .groupByKey(Grouped.with(serdes, sts))
                .aggregate(()-> 0L,
                        (symbol, transaction, counts) -> counts + transaction.getShares(),
                        Materialized.with(serdes, longSerde))
                .toStream()
                .peek((k,v) ->
                        logger.info("symbol: {}, count: {}", k, v))
                .to(appProperties.getProperty("sink.topic"), Produced.with(serdes, longSerde));
        return builder.build();
    }
}
