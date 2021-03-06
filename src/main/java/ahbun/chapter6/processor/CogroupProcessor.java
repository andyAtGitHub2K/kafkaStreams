package ahbun.chapter6.processor;

import ahbun.model.ClickEvent;
import ahbun.model.StockTransaction;
import ahbun.util.Tuple;
import org.apache.kafka.streams.processor.*;
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
    private Punctuator punctuator;

    public CogroupProcessor(String storeName, Duration interval) {
        logger.info("instantiate cogroup processor: duration: " + interval.toString());
        this.storeName = storeName;
        this.interval = interval;
    }

    public Punctuator getPunctuator() {
        return punctuator;
    }

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        kvStore = (KeyValueStore) context().getStateStore(storeName);

        CogroupingPunctuator punctuator
                = new CogroupingPunctuator (context(), kvStore);

        this.context().schedule(interval, PunctuationType.STREAM_TIME, punctuator);
        this.punctuator= punctuator;
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
