package ahbun.topology;

import org.apache.kafka.streams.StreamsBuilder;

public interface TopologyBuilder {
    StreamsBuilder initBuilder(StreamsBuilder streamsBuilder);
}
