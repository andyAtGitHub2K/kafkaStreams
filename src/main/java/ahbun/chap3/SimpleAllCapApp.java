package ahbun.chap3;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import ahbun.lib.JsonSerializer;
import ahbun.lib.StreamsSerdes;
import ahbun.model.Purchase;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class SimpleAllCapApp {
    public static void main() throws IOException, InterruptedException {
        // Load properties
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("chapter3/capitalLetter.properties");
        Properties properties = new Properties();
        properties.load(inputStream);

        // build the stream with the config
        KafkaStreams kafkaStreams = new KafkaStreams(getTopology(properties), properties);

        kafkaStreams.start();
        Thread.sleep(35000);
        kafkaStreams.close();
    }

    private static Topology getTopology(final Properties properties) {
        final StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        final KStream<String, String> input = builder.stream(properties.getProperty("in_topic"),
                Consumed.with(stringSerde, stringSerde));

        ValueMapper<String, String> mapper = String::toUpperCase;
        input
                .mapValues(mapper)
                .to(properties.getProperty("out_topic"), Produced.with(stringSerde, stringSerde));
        return builder.build();
    }
}
