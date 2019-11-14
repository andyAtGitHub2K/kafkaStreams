package ahbun.chapter5;

import ahbun.lib.StreamsSerdes;
import ahbun.lib.TestLib;
import ahbun.model.StockTickerData;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.Properties;

public class TestStockTickerDataTopoplogy {
    private Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private Serde<String> serdes =  Serdes.String();

    @Test
    public void testDriverInputOutput() {
        Properties streamProperties =  new Properties();
        try {
            streamProperties.load(Objects
                    .requireNonNull(
                            ClassLoader
                                    .getSystemResourceAsStream("chapter5/stocktransaction.properties")));

            TopologyTestDriver driver = new TopologyTestDriver(getTopology(streamProperties), streamProperties);
            StockTickerData expectedStockTickerData= new StockTickerData("xyZ", 30.0);
            ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory(serdes.serializer(), serdes.serializer());
            driver.pipeInput(factory.create("stock-stream-topic", expectedStockTickerData.getSymbol(), TestLib.convertToJson(expectedStockTickerData)));

            ProducerRecord<String, String> stockRecord =
                    driver.readOutput("output-stream", serdes.deserializer(),  serdes.deserializer());

            System.out.println(stockRecord.toString());
            System.out.printf("key: %s, value: %s\n", stockRecord.key(), stockRecord.value());
            StockTickerData kafkaSinkOutputFromDriver = gson.fromJson(stockRecord.value(), StockTickerData.class);
            Assert.assertEquals(expectedStockTickerData, kafkaSinkOutputFromDriver);
        } catch (Exception ex) {
            Assert.assertTrue("Test failed: " + ex.getMessage(), false);
        }
    }



    private Topology getTopology(Properties properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        // consume from source
        KStream<String, String> input = builder.stream(properties.getProperty("kstream.topic"),
                Consumed.with(serdes, serdes));
        input.to("output-stream");
        return builder.build();
    }
}
