package ahbun.chapter3;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class YellingAppIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();
    private  final Time mockTime = Time.SYSTEM;
    private final String YELL_A_TOPIC  = "yell-a-topic";
    private final String OUT_TOPIC = "yell-sink";

    private StreamsConfig streamsConfig;
    private Properties producerConfig;
    private KafkaStreams kafkaStreams;
    private Properties consumerConfig;

    @ClassRule
    // execute before() and after() once.
    public static final EmbeddedKafkaCluster EMBEDDED_KAFKA_CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @Before
    public void createTopics() {
        try {

            EMBEDDED_KAFKA_CLUSTER.createTopics(YELL_A_TOPIC, OUT_TOPIC);

        } catch (InterruptedException ex) {
            Assert.fail("create topics failed: " + ex.getMessage());
        }

        setup();
    }

    private void setup() {
        Properties properties = StreamsTestUtils.getStreamsConfig("yellIntegrationTest",
                EMBEDDED_KAFKA_CLUSTER.bootstrapServers(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                new Properties());

        streamsConfig = new StreamsConfig(properties);
        producerConfig = TestUtils.producerConfig(EMBEDDED_KAFKA_CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class);

        consumerConfig = TestUtils.consumerConfig(EMBEDDED_KAFKA_CLUSTER.bootstrapServers(),
                StringDeserializer.class,
                StringDeserializer.class);


        kafkaStreams = new KafkaStreams(buildTopology(), properties);
    }

    @After
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
    }

    @Test
    public void integrationTest() throws Exception {
            List<String> valuesSendToYellApp = Arrays.asList("hello", "how", "are", "you");
            kafkaStreams.start();

            IntegrationTestUtils.produceValuesSynchronously(
                    YELL_A_TOPIC,
                    valuesSendToYellApp,
                    producerConfig,
                    mockTime);

            int expectedMessageCount = valuesSendToYellApp.size();
            List<String> valuesSentToSink = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
                    consumerConfig, OUT_TOPIC, expectedMessageCount);

            Assert.assertEquals(expectedMessageCount, valuesSentToSink.size());

            // verify all input values to Yell topics are capitalized and send to sink, OUT_TOPIC.
            int count = 0;
            for (String value: valuesSentToSink) {
                Assert.assertEquals(valuesSendToYellApp.get(count).toUpperCase(), value);
                count++;
            }
        }

    // convert all input values to capital letters.
    private Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerdes = Serdes.String();
        final KStream<String, String> kStream = builder.stream(YELL_A_TOPIC,
                Consumed.with(stringSerdes, stringSerdes));
        ValueMapper<String, String> valueMapper = String::toUpperCase;
        kStream.mapValues(valueMapper).to(OUT_TOPIC);
        return builder.build();
    }

}
