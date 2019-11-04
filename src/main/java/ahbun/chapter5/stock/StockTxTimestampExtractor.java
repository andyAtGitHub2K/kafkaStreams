package ahbun.chapter5.stock;

import ahbun.model.StockTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

public class StockTxTimestampExtractor  implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        if (! (record.value() instanceof StockTransaction)) {
            return System.currentTimeMillis();
        }

        StockTransaction stockTransaction = (StockTransaction)record.value();
        LocalDateTime time = stockTransaction.getTransactionTimestamp();
        ZoneId zoneId = TimeZone.getDefault().toZoneId();

        return (time != null)
                ? time.toInstant(zoneId.getRules().getOffset(time)).toEpochMilli()
                : record.timestamp();
    }
}
