package ahbun.topology;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;

public interface StreamProcessorPathBuilder<K, V> {
    void attachStreamPath(KStream<K, V> sourceStream);
}
