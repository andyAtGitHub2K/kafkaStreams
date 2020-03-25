package ahbun.chapter9.restore;

import ahbun.chapter9.interactiveQuery.InteractiveServer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;

/***
 * Not Implemented.
 */
public class StateRestoreHttpReporter implements StateRestoreListener {
    private final InteractiveServer interactiveServer;

    public StateRestoreHttpReporter(InteractiveServer interactiveServer) {
        this.interactiveServer = interactiveServer;
    }

    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {

    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {

    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {

    }
}
