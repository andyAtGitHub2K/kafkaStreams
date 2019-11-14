package ahbun.Chapter7.interceptors;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StockTxProducerInterceptor implements ProducerInterceptor<Object, Object> {
    Logger logger = LoggerFactory.getLogger(StockTxProducerInterceptor.class);

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        logger.info("Interceptor onSend: {}", record);

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            logger.error("Interceptor onAcknowledgement: {}", exception);
        } else {
            logger.info("Interceptor onAcknowledgement: {}", metadata);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
