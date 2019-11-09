package ahbun.chapter6.processor;

import ahbun.model.StockPerformance;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.function.Predicate;

/***
 * In each scheduled interval, StockPerformancePunctuator retrieve all the StockPerformance
 * entries from the local KV store and send any entry that passed the predicate test for
 * either the price or volume change level.
 */
public class StockPerformancePunctuator implements Punctuator {
    private static Logger logger = LoggerFactory.getLogger(StockPerformancePunctuator.class);
    private KeyValueStore<String, StockPerformance> localKVStore;
    private ProcessorContext context;
    Predicate<StockPerformance> thresholdCheck;

    /***
     * Constructor
     * @param kvStore local kv store
     * @param context processor context
     * @param predicate for threshold limit check
     */
    public StockPerformancePunctuator(ProcessorContext context,
                                      KeyValueStore<String, StockPerformance> kvStore,
                                      Predicate<StockPerformance> predicate
                                      ) {
        this.context = context;
        this.localKVStore = kvStore;
        this.thresholdCheck = predicate;
    }

    @Override
    /***
     * retrieve all entries from KV store and emits those records that satisfy the price or volume change
     * threshold requirements.
     */
    public void punctuate(long timestamp) {
        KeyValueIterator<String, StockPerformance> iterator = localKVStore.all();
        while(iterator.hasNext()) {
            KeyValue<String, StockPerformance> kv = iterator.next();
            StockPerformance performance = kv.value;

            if (thresholdCheck.test(performance)) {
                context.forward(kv.key, performance);
                performance.sendMessageInstant(Instant.now());
                localKVStore.put(kv.key, performance);
            }
        }
    }
}
