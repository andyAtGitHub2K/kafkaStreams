package ahbun.Chapter7.interceptors;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StateRestoreListener implements org.apache.kafka.streams.processor.StateRestoreListener {
    private static Logger logger = LoggerFactory.getLogger(StateRestoreListener.class);
    private final Map<TopicPartition, Long> totalOffsetToRestore = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> completed = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> totalRecordRestored = new ConcurrentHashMap<>();
    private static NumberFormat numberFormat = new DecimalFormat("#.##");
    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        long offsetToRestore = endingOffset - startingOffset;
        totalOffsetToRestore.put(topicPartition, offsetToRestore);
        logger.info("Restore start with total: " + offsetToRestore);
    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        completed.put(topicPartition, batchEndOffset);
        long total = totalOffsetToRestore.get(topicPartition);
        long remaining = total - batchEndOffset;
        totalRecordRestored.put(topicPartition, totalRecordRestored.getOrDefault(topicPartition,0L) + numRestored);
        logger.info("PCT remaining to complete: ", numberFormat.format((double)(remaining / total) * 100.0 ));
    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        logger.info("onRestoreEnd finished and restored partition-{}:  {}", topicPartition.partition(),  totalRestored);
    }
}
