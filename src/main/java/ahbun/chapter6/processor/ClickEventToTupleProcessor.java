package ahbun.chapter6.processor;

import ahbun.model.ClickEvent;
import ahbun.model.StockTransaction;
import ahbun.util.Tuple;
import org.apache.kafka.streams.processor.AbstractProcessor;

public class ClickEventToTupleProcessor extends AbstractProcessor<String, ClickEvent> {
    @Override
    public void process(String key, ClickEvent value) {
        if (key != null) {
            Tuple<ClickEvent, StockTransaction> tuple = new Tuple<>(
                    value, null);
            context().forward(key, tuple);
        }
    }
}
