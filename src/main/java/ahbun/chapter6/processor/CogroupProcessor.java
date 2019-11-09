package ahbun.chapter6.processor;

import ahbun.lib.ObjectConverter;
import ahbun.lib.StreamsSerdes;
import ahbun.model.ClickEvent;
import ahbun.model.StockTransaction;
import ahbun.util.Tuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/***
 * CogroupProcessor joins to events by common key in a local store
 */
public class CogroupProcessor extends
        AbstractProcessor<String, Tuple<ClickEvent, StockTransaction>> {
    private static Logger logger = LoggerFactory.getLogger(CogroupProcessor.class);
    private KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> kvStore;
    private String storeName;
    private Duration interval;

    public CogroupProcessor(String storeName, Duration interval) {
        logger.info("instantiate cogroup processor: duration: " + interval.toString());
        this.storeName = storeName;
        this.interval = interval;
    }

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        kvStore = (KeyValueStore) context().getStateStore(storeName);
        // setup schedular
//        CogroupingPunctuator punctuator
//                = new CogroupingPunctuator (context(), kvStore);
//        //= new CogroupingPunctuator (context(), storeName);
//        logger.info("Scheduled for " + interval.toString());
//        context().schedule(interval, PunctuationType.STREAM_TIME, punctuator);

        this.context().schedule(interval, PunctuationType.STREAM_TIME, (timestamp) -> {
            KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iteratorDebug =  this.kvStore.all();
            int count=0;
            while(iteratorDebug.hasNext()) {
                iteratorDebug.next();
                count++;
            }
            iteratorDebug.close();
            KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iterator =  this.kvStore.all();

            logger.info("Punctuator executed");
            while (iterator.hasNext()) {
                KeyValue<String, Tuple<List<ClickEvent>, List<StockTransaction>>> keyValue = iterator.next();

                if (keyValue.value != null &&
                        (!keyValue.value.getX().isEmpty() || !keyValue.value.getY().isEmpty())) {
                    List<ClickEvent> clickEventList = new ArrayList<>(keyValue.value.getX());
                    List<StockTransaction> stockTransactionList = new ArrayList<>(keyValue.value.getY());

                    Tuple<List<ClickEvent>, List<StockTransaction>> newTuple =
                            new Tuple<>(clickEventList, stockTransactionList);
                    String json = ObjectConverter.convertToJson(newTuple);

                    context.forward(keyValue.key, json);
                    keyValue.value.getX().clear();
                    keyValue.value.getY().clear();
                    this.kvStore.put(keyValue.key, keyValue.value);
                }
            }
            iterator.close();
            context.commit();
        });
    }

    @Override
    public void process(String key, Tuple<ClickEvent, StockTransaction> value) {
        if (key != null) {
            Tuple<List<ClickEvent>, List<StockTransaction>> tuple = kvStore.get(key);

            if (tuple == null) {
                tuple = new Tuple<>(new ArrayList<>(), new ArrayList<>());
            }

            if (value.getX() != null) {
                tuple.getX().add(value.getX());
            }
            if (value.getY() != null) {
                tuple.getY().add(value.getY());
            }
            kvStore.put(key, tuple);
            context().commit();
        }
    }
}
