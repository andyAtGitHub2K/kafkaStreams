package ahbun.chapter6.processor;

import ahbun.model.StockPerformance;
import ahbun.model.StockTransaction;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Predicate;
import java.util.function.Supplier;

/***
 * StockPerformanceProcessor periodically updates the local store
 * of the StockPerformance containing share and volume statistics and trends derived from
 * the input StockTransaction.
 */
public class StockPerformanceProcessor extends AbstractProcessor<String, StockTransaction> {
    private static Logger logger = LoggerFactory.getLogger(StockPerformanceProcessor.class);
    private KeyValueStore<String, StockPerformance> localKVStore;
    private Supplier<StockPerformance> stockPerformanceSupplier;
    private Predicate<StockPerformance> performancePredicate;
    private String localStoreName;
    private Duration processInterval;

    /***
     * StockPerformanceProcessor Constructor.
     * @param localStoreName name of the local KV store
     * @param processInterval time interval in second to execute the processor
     * @param stockPerformanceSupplier StockPerformance supplier
     * @param performancePredicate check if current relative change requirement satisfied
     */
    public StockPerformanceProcessor(
            String localStoreName,
            Duration processInterval,
            Supplier<StockPerformance> stockPerformanceSupplier,
            Predicate<StockPerformance> performancePredicate) {
        this.localStoreName = localStoreName;
        this.stockPerformanceSupplier = stockPerformanceSupplier;
        this.processInterval = processInterval;
        this.performancePredicate = performancePredicate;
    }

    @Override
    /***
     * obtain the local KV store by name and schedule processor at processInterval.
     */
    public void init(ProcessorContext processorContext) {
        super.init(processorContext);

        // obtain the local KV store by name
        localKVStore = (KeyValueStore<String, StockPerformance>) context().getStateStore(localStoreName);

        // schedule a punctuator to run the process
        StockPerformancePunctuator performancePunctuator =
                new StockPerformancePunctuator(context(), localKVStore, performancePredicate);
        context().schedule(processInterval, PunctuationType.WALL_CLOCK_TIME, performancePunctuator);
    }

    @Override
    /***
     * process performs updates the price/volume statistics in the following steps:
     * 1. retrieve the symbol from the kv store
     * 2. create if not found
     * 3. perform calculations
     * 4. update results in the kv store
     */
    public void process(String stockSymbol, StockTransaction tx) {
        if (stockSymbol == null ) {
            System.exit(-1);
        }

        if (stockSymbol != null) {
            // 1. retrieve the symbol from the kv store
            StockPerformance performance = localKVStore.get(stockSymbol);

            // 2. create if not found
            if (performance == null) {
                performance =  stockPerformanceSupplier.get();
            }

            // 3. perform calculations
            performance.update(tx.getSharePrice(), tx.getShares());
            // 4. update results in the kv store
            localKVStore.put(stockSymbol, performance);
        }
    }
}
