package ahbun.chapter6.processor;

import ahbun.model.ClickEvent;
import ahbun.model.StockTransaction;

import ahbun.util.Tuple;
import org.apache.kafka.streams.processor.AbstractProcessor;

/***
 * Maps StockTransaction to Tuple<stock_symbol, StockTransaction>>
 */
public class StockTxToTupleProcessor extends AbstractProcessor<String, StockTransaction> {

    @Override
    public void process(String key, StockTransaction value) {
        if (key != null) {
            Tuple<ClickEvent, StockTransaction> tuple =
                    new Tuple(null, value);
            context().forward(key, tuple);
        }
    }
}
