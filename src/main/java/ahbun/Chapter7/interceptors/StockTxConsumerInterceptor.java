package ahbun.Chapter7.interceptors;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class StockTxConsumerInterceptor implements ConsumerInterceptor<Object, Object> {
    Logger logger = LoggerFactory.getLogger(StockTxConsumerInterceptor.class);

    @Override
    public ConsumerRecords<Object, Object> onConsume(ConsumerRecords<Object, Object> records) {
        logger.info("intercept onConsume: {}", buildMessage(records.iterator()));
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        logger.info("intercept onCommit: {}", offsets);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    private String buildMessage(Iterator<ConsumerRecord<Object, Object>> consumerRecordIterator) {
        StringBuilder stringBuilder = new StringBuilder();
        while (consumerRecordIterator.hasNext()) {
            stringBuilder.append(consumerRecordIterator.next());
        }

        return stringBuilder.toString();
    }
}
