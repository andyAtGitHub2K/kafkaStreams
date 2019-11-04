package ahbun.lib;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Map;

/***
 * JsonSerializer converts the object to json then transforms it into byte
 * array.
 * @param <T>
 */
public class JsonSerializer<T> implements Serializer<T> {
    private static Logger logger = LoggerFactory.getLogger(JsonSerializer.class);
    private Gson gson;

    public JsonSerializer() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(FixedSizePriorityQueue.class, new FPQTypeAdapter().nullSafe());
        gson = builder.create();
    }
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, T data) {
        logger.debug(data.getClass().getName());
        logger.debug(gson.toJson(data));
        return gson.toJson(data).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public void close() {

    }
}
