package ahbun.chapter8;

import ahbun.lib.StreamsSerdes;
import ahbun.model.Purchase;
import ahbun.model.PurchasePattern;
import ahbun.model.Reward;
import ahbun.util.DataGenerator;
import ahbun.util.PurchaseSimiluationRecipe;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/***
 * ZMartTopologyTest uses TopologyTestDriver to verify the ZMart Topology
 * by feeding input to the source and verify the expected output from the sinks
 * independent of Kafka & Zookeeper.
 */
public class ZMartTopologyKeyStoreTest {
    private TopologyTestDriver testDriver;
    private Serde<String> stringSerde = Serdes.String();
    private StreamsSerdes.PurchaseSerde purchaseSerde = new StreamsSerdes.PurchaseSerde();
    private StreamsSerdes.PurchasePatternSerde purchasePatternSerde = new StreamsSerdes.PurchasePatternSerde();
    private StreamsSerdes.RewardSerde rewardSerde = new StreamsSerdes.RewardSerde();
    private ConsumerRecordFactory<String, Purchase> consumerPurchaseRecordFactory = new ConsumerRecordFactory(stringSerde.serializer(), purchaseSerde.serializer());
    private PurchaseSimiluationRecipe recipe = new PurchaseSimiluationRecipe(5, 3, 5);
    private List<Purchase> expectedPurchaseList;
    Purchase cafePurchase;
    Purchase electronicsPurchase;
    private String sourceTopic;
    private String rewardTopic;
    private String patternTopic;
    private String cafeSink;
    private String electronicsSink;
    private String sinkTopic;

    @Before
    public void setup(){
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream inputStream = classLoader.getResourceAsStream("chapter3/zeemart.properties");
            Properties kafkaStreamProperties = new Properties();
            kafkaStreamProperties.load(inputStream);

            InputStream businessInputStream = classLoader.getResourceAsStream("chapter3/business.properties");
            Properties businessProperties = new Properties();
            businessProperties.load(businessInputStream);

            // initialize topics
            sourceTopic = kafkaStreamProperties.getProperty("in_topic");
            rewardTopic = kafkaStreamProperties.getProperty("reward_topic");
            patternTopic = kafkaStreamProperties.getProperty("pattern_topic");
            cafeSink = kafkaStreamProperties.getProperty("cafe_sink");
            electronicsSink = kafkaStreamProperties.getProperty("electronics_sink");
            sinkTopic = kafkaStreamProperties.getProperty("out_topic");

            initCafePurchase();
            initElectronicsPurchase();
            testDriver = new TopologyTestDriver(ZMartTopology.getKeyStoreTopolopgy(kafkaStreamProperties, businessProperties), kafkaStreamProperties);
        } catch (IOException | ParseException ex) {
            Assert.assertTrue("setup failed: " + ex.getMessage(), false);
        }
    }

    private void initCafePurchase() throws ParseException {
        Purchase.PurchaseBuilder builder = Purchase.builder();
        builder.creditCardNumber("1234-5678-1234-5678");
        builder.customerId("cid-123");
        builder.department("cafe");
        builder.employeeId("eid-001");
        builder.firstName("fname");
        builder.lastName("lname");
        builder.itemPurchased("latte");
        builder.price(100.00);
        builder.purchaseDate(LocalDateTime.now());
        builder.quantity(1);
        builder.storeId("store-01");
        builder.zipCode("12345");

        cafePurchase = builder.build();
    }

    private void initElectronicsPurchase() throws ParseException {
            Purchase.PurchaseBuilder builder = Purchase.builder();
            builder.creditCardNumber("1234-5678-1234-5678");
            builder.customerId("cid-123");
            builder.department("electronics");
            builder.employeeId("eid-001");
            builder.firstName("fname");
            builder.lastName("lname");
            builder.itemPurchased("pc");
            builder.price(200.00);
            builder.purchaseDate(LocalDateTime.now());
            builder.quantity(2);
            builder.storeId("store-01");
            builder.zipCode("12345");

            electronicsPurchase = builder.build();
    }

    private void initPurchase() {
        try {
            expectedPurchaseList = DataGenerator.createPurchase(recipe);
        } catch (ParseException ex) {
            Assert.assertTrue("create purchase failed: " + ex.getMessage(), false);
        }
    }

    @After
    public void teardown() {
        if (testDriver != null) {
            System.out.println("test driver closed");
            testDriver.close();
        }
    }

    @Test
    public void testMaskedCreditCardOutputTopic() {
        initPurchase();

        // create consumer record
        Purchase expectedPurchase = expectedPurchaseList.get(0);
        ;
        // feed record to topic
        testDriver.pipeInput(consumerPurchaseRecordFactory.create(sourceTopic, null, expectedPurchase));

        // read output from masked credit card sink
        ProducerRecord<String, Purchase> purchaseProducerRecord =
                testDriver.readOutput(sinkTopic, stringSerde.deserializer(), purchaseSerde.deserializer());

        Purchase purchaseRecord = purchaseProducerRecord.value();
        String originalCreditCardNumber = expectedPurchase.getCreditCardNumber();
        String maskedCreditCardNumber = purchaseRecord.getCreditCardNumber();

        Assert.assertNotEquals(expectedPurchase, purchaseRecord);
        Assert.assertNotEquals(originalCreditCardNumber, maskedCreditCardNumber);
        Assert.assertEquals(originalCreditCardNumber.substring(originalCreditCardNumber.length() - 5),
                maskedCreditCardNumber.substring(originalCreditCardNumber.length() - 5));
    }

    @Test
    public void testRewardTopic() {
        initPurchase();
        // verify resulting reward record of each input purchase
        Map<String, Integer> rewardStore = new HashMap<>();
        int newRewardPoints;
        for(Purchase expectedPurchase: expectedPurchaseList) {
            // feed record to topic
            testDriver.pipeInput(consumerPurchaseRecordFactory.create(sourceTopic, null, expectedPurchase));

            ProducerRecord<String, Reward> rewardProducerRecord = testDriver.readOutput(rewardTopic, stringSerde.deserializer(), rewardSerde.deserializer());
            Reward resultingReward = rewardProducerRecord.value();

            // verify reward record against the originating purchase record
            String custumerID = expectedPurchase.getLastName() + "," + expectedPurchase.getFirstName();
            Assert.assertEquals(custumerID, resultingReward.getCustumerID());
            Assert.assertTrue(Math.abs(expectedPurchase.getPrice() * expectedPurchase.getQuantity() -
                    resultingReward.getPurchaseTotal()) < 0.0001);

            if (!rewardStore.containsKey(custumerID)) {
                Assert.assertTrue((int) (expectedPurchase.getPrice() * expectedPurchase.getQuantity()) == resultingReward.getTotalPurchasePoint());
                rewardStore.put(custumerID, resultingReward.getTotalPurchasePoint());
            } else {
                newRewardPoints = (int) (expectedPurchase.getPrice() * expectedPurchase.getQuantity());
                Assert.assertTrue(rewardStore.get(custumerID) + newRewardPoints - resultingReward.getTotalPurchasePoint() == 0);
                rewardStore.put(custumerID, resultingReward.getTotalPurchasePoint());
            }
        }
    }

    @Test
    public void testPatternTopic() {
        initPurchase();
        for (Purchase expectedPurchase: expectedPurchaseList) {
            testDriver.pipeInput(consumerPurchaseRecordFactory.create(sourceTopic, null, expectedPurchase));

            ProducerRecord<String, PurchasePattern> purchasePatternProducerRecord =
                    testDriver.readOutput(patternTopic, stringSerde.deserializer(),
                            purchasePatternSerde.deserializer());

            PurchasePattern purchasePattern = purchasePatternProducerRecord.value();
            Assert.assertEquals(expectedPurchase.getZipCode(), purchasePattern.getZipCode());
            Assert.assertEquals(expectedPurchase.getItemPurchased(), purchasePattern.getItem());
            Assert.assertEquals(expectedPurchase.getPurchaseDate(), purchasePattern.getDate());
            Assert.assertTrue(Math.abs(expectedPurchase.getQuantity() * expectedPurchase.getPrice() -
                    purchasePattern.getAmount()) < 0.001);
        }
    }

    @Test
    public void testCafeSink() {
        Purchase expectedPurchase = cafePurchase;

        testDriver.pipeInput(consumerPurchaseRecordFactory.create(sourceTopic, null, expectedPurchase));
        ProducerRecord<String, Purchase> cafePurchaseRecord =
                    testDriver.readOutput(cafeSink, stringSerde.deserializer(),
                        purchaseSerde.deserializer());

        Assert.assertNotNull(cafePurchase);
        Assert.assertNotNull(cafePurchaseRecord);
        Assert.assertEquals(expectedPurchase.getItemPurchased(), cafePurchaseRecord.value().getItemPurchased());
        Assert.assertEquals(expectedPurchase.getPurchaseDate(), cafePurchaseRecord.value().getPurchaseDate());
        Assert.assertEquals(expectedPurchase.getZipCode(), cafePurchaseRecord.value().getZipCode());
        Assert.assertEquals(expectedPurchase.getCustomerId(), cafePurchaseRecord.value().getCustomerId());

        ProducerRecord<String, Purchase> electronicsPurchaseRecord =
                testDriver.readOutput(electronicsSink, stringSerde.deserializer(),
                        purchaseSerde.deserializer());

        Assert.assertNull(electronicsPurchaseRecord);
    }

    @Test
    public void testElectronicsSink() {
        Purchase expectedPurchase = electronicsPurchase;
        testDriver.pipeInput(consumerPurchaseRecordFactory.create(sourceTopic, null, expectedPurchase));
        ProducerRecord<String, Purchase> electronicsPurchaseRecord =
                testDriver.readOutput(electronicsSink, stringSerde.deserializer(),
                        purchaseSerde.deserializer());
        Assert.assertNotNull(electronicsPurchase);
        Assert.assertNotNull(electronicsPurchaseRecord);
        Assert.assertEquals(expectedPurchase.getItemPurchased(), electronicsPurchaseRecord.value().getItemPurchased());
        Assert.assertEquals(expectedPurchase.getPurchaseDate(), electronicsPurchaseRecord.value().getPurchaseDate());
        Assert.assertEquals(expectedPurchase.getZipCode(), electronicsPurchaseRecord.value().getZipCode());
        Assert.assertEquals(expectedPurchase.getCustomerId(), electronicsPurchaseRecord.value().getCustomerId());

        ProducerRecord<String, Purchase> cafePurchaseRecord =
                testDriver.readOutput(cafeSink, stringSerde.deserializer(),
                        purchaseSerde.deserializer());
        Assert.assertNull(cafePurchaseRecord);
    }
}