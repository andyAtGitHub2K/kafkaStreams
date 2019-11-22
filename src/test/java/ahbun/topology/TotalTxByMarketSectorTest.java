package ahbun.topology;

import ahbun.lib.StreamsSerdes;
import ahbun.model.StockTransaction;
import ahbun.util.DataGenerator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TotalTxByMarketSectorTest {
    private TopologyTestDriver testDriver;
    private ConsumerRecordFactory<String, StockTransaction> transactionConsumerRecordFactory;
    private Serde<String> stringSerde = Serdes.String();
    private Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
    private Serde<Long> longSerde = Serdes.Long();
    private List<StockTransaction> transactionList;
    private String[] INDUSTRY_LIST = {"food", "book", "sales"};
    private String sourceTopic;
    private String sinkTopic;

    @Before
    public void setup() {
        try {
            String appPropertiesPath = "chapter9/kafkaTxByIndustrySectorAppConfig.properties";
            TotalTxByMarketSector totalTxByMarketSector = new TotalTxByMarketSector(appPropertiesPath);
            transactionList = DataGenerator.makeStockTx(5, 3, INDUSTRY_LIST);
            sourceTopic = totalTxByMarketSector.sourceTopic();
            sinkTopic = totalTxByMarketSector.sinkTopic();
            Properties streamProperties = new Properties();
            String streamPropertiesPath = "chapter9/kafka_producer_stock.properties";
            streamProperties
                    .load(Objects
                            .requireNonNull(
                                    ClassLoader
                                            .getSystemResourceAsStream(streamPropertiesPath)));
            StreamsBuilder builder = new StreamsBuilder();
            Topology topology = totalTxByMarketSector.initBuilder(builder).build();
            testDriver = new TopologyTestDriver(topology, streamProperties);
            transactionConsumerRecordFactory = new ConsumerRecordFactory<>(sourceTopic, stringSerde.serializer(), stockTransactionSerde.serializer());
        } catch (IOException ex) {
            Assert.fail(ex.getMessage());
        }
    }

    @After
    public void teardown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    public void initBuilder() {
        Map<String, Long> accumulator = new HashMap<>();
        String expectedOutput;
        String industry;
        long shares;
        for (StockTransaction transaction: transactionList) {
            industry = transaction.getIndustry();
            shares = transaction.getShares();

            // update accumulator
            if (accumulator.containsKey(transaction.getIndustry())) {
                accumulator.put(industry, accumulator.get(industry) + shares);
            } else {
                accumulator.put(industry, shares);
            }

            expectedOutput = String.format("%s : %d", industry, accumulator.get(industry));

            testDriver.pipeInput(transactionConsumerRecordFactory.create(sourceTopic, null, transaction));
            ProducerRecord<String, Long> producerRecord =
                    testDriver.readOutput(sinkTopic, stringSerde.deserializer(), longSerde.deserializer());
            Assert.assertEquals(expectedOutput, String.format("%s : %d", producerRecord.key(), producerRecord.value()));
        }
    }
}