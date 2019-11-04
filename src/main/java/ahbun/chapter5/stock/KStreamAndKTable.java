package ahbun.chapter5.stock;

import ahbun.lib.StreamsSerdes;
import ahbun.model.StockTickerData;
import ahbun.util.MockDataProducer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * KStreamAndKTable demonstrates the data processing differences between
 * KStream and KTable. In KStream, data is treated as independent events
 * similar to insert in database world. On the other hand, KTable is purely
 * a update event. One requirement is that key must existed before processing
 * data.
 */
public class KStreamAndKTable {
    public static void main(String[] argc) throws IOException {
        /**
         * 1. create a KStream / KTable for the StockTickerData to print the
         *    ingested data to console.
         * 2. Starts the MockGenerator
         * 3. shutdown.
         */

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("chapter5/stocktransaction.properties");
        Properties properties = new Properties();
        properties.load(inputStream);
        StreamsSerdes.StockTickerSerde sts = new StreamsSerdes.StockTickerSerde();
        MockDataProducer.produceStockData(3, "stock-stream-topic", "stock-table-topic", 4000);
        KafkaStreams kafkaStreams = new KafkaStreams(getTopology(properties), properties);

        try {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            Thread.sleep(15000);
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        } finally {
            kafkaStreams.close();
            MockDataProducer.shutdown();
        }
    }

    private static Topology getTopology(Properties properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        StreamsSerdes.StockTickerSerde sts = new StreamsSerdes.StockTickerSerde();
        Serde<String> serdes =  Serdes.String();

        // consume from source
        KStream<String, StockTickerData> input = builder.stream(properties.getProperty("kstream.topic"),
                Consumed.with(serdes, sts));

        KTable<String, StockTickerData>  kTable = builder.table(properties.getProperty("ktable.topic"),
                Materialized.as("myStore"));
        kTable.toStream().print(Printed.<String, StockTickerData >toSysOut().withLabel("kTable"));
        input.print(Printed.<String, StockTickerData >toSysOut().withLabel("kStream"));

        return builder.build();
    }
}
