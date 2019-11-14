package ahbun.chapter8;

import ahbun.lib.StreamsSerdes;
import ahbun.model.ShareVolume;
import ahbun.model.StockTransaction;
import ahbun.util.DataGenerator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;


public class StockStateStoreTest {
    private static Logger logger = LoggerFactory.getLogger(StockStateStoreTest.class);
    private TopologyTestDriver testDriver;

    private Serde<StockTransaction> stockTransactionSerde =  StreamsSerdes.StockTransactionSerde();
    private Serde<String> stringSerde = Serdes.String();

    private ConsumerRecordFactory<String, StockTransaction> consumerRecordFactory;
    private List<StockTransaction> transactionList;
    private String[] INSUSTRY_LIST = {"food", "book", "sales"};

    private String source;
    private String sink;

    @Before
    public void setup() {
        try {
            Properties streamProperties = new Properties();
            streamProperties.load(Objects
                    .requireNonNull(
                            ClassLoader
                                    .getSystemResourceAsStream("chapter5/kafkaTop5tradeVolumeStreamConfig.properties")));

            Properties appProperties = new Properties();
            appProperties.load(Objects
                    .requireNonNull(
                            ClassLoader
                                    .getSystemResourceAsStream("chapter5/kafkaTopVolumeAppConfig.properties")));


            testDriver = new TopologyTestDriver(StockTop5Topology.getTopology(appProperties, logger), streamProperties);

            transactionList = DataGenerator.makeStockTx(5, 3, INSUSTRY_LIST);
            consumerRecordFactory =
                    new ConsumerRecordFactory<>(stringSerde.serializer(), stockTransactionSerde.serializer());

            source = appProperties.getProperty("share.volume.topic");
            sink = appProperties.getProperty("topN.by.industry");
        } catch (IOException ex) {
            Assert.assertTrue("setup failed: " + ex.getMessage(), false);
        }
    }

    @After
    public void teardown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    public void testStateStore() {

        StockTransaction transaction = transactionList.get(0);
        String expectedOutput = String.format("[%s]: 1)%s:%d",
                transaction.getIndustry(), transaction.getSymbol(), transaction.getShares());
        testDriver.pipeInput(consumerRecordFactory.create(source, null, transaction));
        ProducerRecord<String, String> producerRecord = testDriver.readOutput(sink,stringSerde.deserializer(), stringSerde.deserializer());
        Assert.assertEquals(expectedOutput, producerRecord.value().trim());
    }
}
