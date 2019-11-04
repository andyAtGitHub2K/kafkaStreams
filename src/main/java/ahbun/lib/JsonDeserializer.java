package ahbun.lib;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Map;

/***
 *
 * @param <T>
 */
public class JsonDeserializer<T> implements Deserializer<T> {
    private Gson gson;
    private Class<T> deserializedClass;
    private Type reflectionTypeToken;
    private Logger logger = LoggerFactory.getLogger(JsonDeserializer.class);

    public JsonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
        init();
    }

    public JsonDeserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
        init();
    }

    private void init() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(FixedSizePriorityQueue.class,
                new FPQTypeAdapter().nullSafe());
        gson = builder.create();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (deserializedClass == null) {
            deserializedClass =(Class<T>) configs.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        Type deserializeFrom = deserializedClass != null ? deserializedClass : reflectionTypeToken;
        logger.debug("deserialize = " + deserializeFrom.getTypeName());
        return gson.fromJson(new String(data),deserializeFrom);
    }

    @Override
    public void close() {

    }
}
