package ahbun.model;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class PurchaseJoiner implements ValueJoiner<Purchase, Purchase, CorrelatedPurchase> {
    @Override
    public CorrelatedPurchase apply(Purchase value1, Purchase value2) {
        CorrelatedPurchase.CorrelatedPurchaseBuilder builder =
                CorrelatedPurchase.builder(value1, value2);
        return builder.build();
    }
}
