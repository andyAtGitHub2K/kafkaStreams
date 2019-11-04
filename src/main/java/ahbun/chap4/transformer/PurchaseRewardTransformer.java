package ahbun.chap4.transformer;

import ahbun.model.Purchase;
import ahbun.model.RewardAccumulator;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

/***
 * Transform the Purchase type to RewardAccumulator.
 */
public class PurchaseRewardTransformer implements ValueTransformer<Purchase, RewardAccumulator> {
    private KeyValueStore<String, Integer> stateStore;
    private final String storeName;
    private ProcessorContext context; // a local reference to ProcessorContext

    public PurchaseRewardTransformer(String storeName) {
        Objects.requireNonNull(storeName, "Store name cannot be null");
        this.storeName = storeName;
    }

    @Override
    // initialize the context and through which obtain the state store by store name.
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore) this.context.getStateStore(storeName);
    }

    @Override
    /***
     * Transformation steps:
     *    1. get the reward points accumulated by the customer ID
     *    2. compute the total = accumulated + current
     *    3. update the state in local store
     *    4. create the RewardAccumulator with update state
     */
    public RewardAccumulator transform(Purchase value) {
        // 1.
        Integer accumulatedValue = stateStore.get(value.getCustomerId());
        // 2.
        RewardAccumulator accumulator = RewardAccumulator.builder(value).build();
        if (accumulatedValue != null) {
            accumulator.addRewardPoints(accumulatedValue);
        }
        // 3.
        stateStore.put(value.getCustomerId(), accumulator.getTotalPurchasePoint());

        // 4.
        return accumulator;
    }

    @Override
    public void close() {

    }
}
