package ahbun.chapter6.processor;

import ahbun.model.ClickEvent;
import ahbun.model.StockTransaction;
import ahbun.util.Tuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CogroupingPunctuator implements Punctuator {
    private ProcessorContext context;
    private KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> kvKeyValueStore;
    private static Logger logger = LoggerFactory.getLogger(CogroupingPunctuator.class);

    public CogroupingPunctuator(ProcessorContext context,
                                KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> kstore) {
        this.context = context;
        this.kvKeyValueStore = kstore;
    }
    public CogroupingPunctuator(ProcessorContext context, String storeName
                                ) {
        this.context = context;
        this.kvKeyValueStore = (KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> )context.getStateStore(storeName);
    }
    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iteratorDebug =  kvKeyValueStore.all();
        int count=0;
        while(iteratorDebug.hasNext()) {
            iteratorDebug.next();
            count++;
        }
        iteratorDebug.close();
        logger.debug("KV store size: " + count);
       KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iterator =  kvKeyValueStore.all();

       logger.debug("Punctuator executed");
       while (iterator.hasNext()) {
           KeyValue<String, Tuple<List<ClickEvent>, List<StockTransaction>>> keyValue = iterator.next();
                if (keyValue.value != null) {
                    if (!keyValue.value.getX().isEmpty()) {
                        logger.debug("Click Event list: " + keyValue.value.getX());
                    }
                    if (!keyValue.value.getY().isEmpty()) {
                        logger.debug("Transaction list: " + keyValue.value.getY());
                    }

                } else {
                    logger.error("value is NULL");
                }
               if (keyValue.value != null &&
                       (!keyValue.value.getX().isEmpty() || !keyValue.value.getY().isEmpty())) {
                   List<ClickEvent> clickEventList = new ArrayList<>(keyValue.value.getX());
                   List<StockTransaction> stockTransactionList = new ArrayList<>(keyValue.value.getY());
                   context.forward(keyValue.key, new Tuple<>(clickEventList, stockTransactionList));
                   keyValue.value.getX().clear();
                   keyValue.value.getY().clear();
                   kvKeyValueStore.put(keyValue.key, keyValue.value);
           }
       }
       iterator.close();
       context.commit();
    }
}
