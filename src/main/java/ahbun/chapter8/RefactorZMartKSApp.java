package ahbun.chapter8;

import org.apache.kafka.streams.KafkaStreams;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class RefactorZMartKSApp {
    enum STREAM_BRANCH {
        CAFE,
        ELECTRONICS
    }

    public static void main(String[] argv) throws IOException, InterruptedException {
        // Load properties
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("chapter3/zeemart.properties");
        Properties kafkaStreamProperties = new Properties();
        kafkaStreamProperties.load(inputStream);

        InputStream businessInputStream = classLoader.getResourceAsStream("chapter3/business.properties");
        Properties businessProperties = new Properties();
        businessProperties.load(businessInputStream);

        KafkaStreams kafkaStreams =
                new KafkaStreams(ZMartTopology.getTopology(kafkaStreamProperties, businessProperties),
                        kafkaStreamProperties);
        kafkaStreams.start();
        Thread.sleep(20000);
        kafkaStreams.close();
    }
}
