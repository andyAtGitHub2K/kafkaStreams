package ahbun.mocks;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.KeyValueIteratorStub;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockKeyValueStore<K, V> implements KeyValueStore<K, V> {
    private Map<K, V> internalStore = new HashMap<>();
    private String name;
    private boolean open;

    public MockKeyValueStore() {
        name = "mockKVStore";
        open = true;
    }

    @Override
    public void put(K key, V value) {
        internalStore.put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        if (!internalStore.containsKey(key)) {
            internalStore.put(key, value);
        }

        return internalStore.get(key);
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        if (entries != null) {
            for (KeyValue<K, V> kv : entries) {
                internalStore.put(kv.key, kv.value);
            }
        }
    }

    @Override
    public V delete(K key) {
        return internalStore.remove(key);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        open = false;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public V get(K key) {
        return internalStore.getOrDefault(key, null);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return null;
    }

    @Override
    public KeyValueIterator<K, V> all() {
        List<KeyValue<K, V>> keyValueList = new ArrayList<>();
        for (Map.Entry<K, V> entry: internalStore.entrySet()) {
            keyValueList.add(KeyValue.pair(entry.getKey(), entry.getValue()));
        }
        return new KeyValueIteratorStub<>(keyValueList.iterator());
    }

    @Override
    public long approximateNumEntries() {
        return internalStore.size();
    }
}
