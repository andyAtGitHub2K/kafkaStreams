package ahbun.chapter9.interactiveQuery;

import ahbun.util.WebPath;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import java.util.Map;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.staticFiles;

public class InteractiveServer {
    private Logger logger = LoggerFactory.getLogger(InteractiveServer.class);
    private HostInfo hostInfo;
    private KafkaStreams kafkaStreams;
    private boolean ready = false;
    // to access store from remote node
    private Client client = ClientBuilder.newClient();
    private static final String STORE_PARAM = ":store";
    private static final String KEY_PARAM = ":key";
    private static final String FROM_PARAM = ":from";
    private static final String TO_PARAM = ":to";
    private static final String STORES_NOT_ACCESSIBLE = "{\"message\":\"Stores not ready for service, probably re-balancing\"}";
    private static final Serde<String> stringSerde = Serdes.String();
    private static final String KV_PATH = "kv";

    public InteractiveServer(final KafkaStreams kafkaStreams, final HostInfo hostInfo) {
        this.kafkaStreams = kafkaStreams;
        this.hostInfo = hostInfo;

        configureWebServer();
        setupServiceURLs();
    }

    public synchronized void setReady(boolean ok) {
        this.ready = ok;
    }

    public void stop() {
        for (int i = 0; i < 10; i++) {
            logger.info("shut down");
        }
        Spark.stop();
        client.close();
    }

    private void configureWebServer() {
        // set the folder in classpath serving static files
        staticFiles.location("/webserver");
        // set the listening port of the web server
        port(hostInfo.port());
    }

    private void setupServiceURLs() {
        // setup service URLs
        get(WebPath.URLs.INTERACTIVE_QUERY_APP, (req, res) -> {
            res.redirect(WebPath.HTMLs.INTERACTIVE_QUERY_APP);
            return "";
        });
        get(WebPath.URLs.GET_KV_FROM_STORE, (req, res) ->
           ready ? fetchKeyValueFromStore(req.params()) : STORES_NOT_ACCESSIBLE);
    }

    /***
     * http://localhost:4567/kv/total-shares-by-industry-store/Mining
     * @param params
     * @return
     */
    private String fetchKeyValueFromStore(Map<String, String> params) {
        String store = params.get(STORE_PARAM);
        String key = params.get(KEY_PARAM);

        HostInfo storeHostInfo = getHostInfo(store, key);

        if (storeHostInfo.host().equals("unknown")) {
            return STORES_NOT_ACCESSIBLE;
        }

        if (isRemoteStore(storeHostInfo)) {
            return fetchRemote(storeHostInfo, KV_PATH, params);
        }

        ReadOnlyKeyValueStore<String, Long> readOnlyKeyValueStore =  kafkaStreams.store(store, QueryableStoreTypes.keyValueStore());  //.getKeyValueStore("total-shares-by-industry-store");
        Long value  = readOnlyKeyValueStore.get(key);

        return key + ":" + value;
    }

    private boolean isRemoteStore(HostInfo selectedHostInfo) {
        return !this.hostInfo.equals(selectedHostInfo);
    }

    /***
     * obtain the host info for the store having the key
     * @param store store name
     * @param key key
     * @return {@link HostInfo}
     */
    private HostInfo getHostInfo(String store, String key) {
        StreamsMetadata metadata = kafkaStreams.metadataForKey(store, key, stringSerde.serializer());
        return metadata.hostInfo();
    }

    /***
     * retrieve value from remote key-value store
     * @param hostInfo {@link HostInfo}
     * @param path
     * @param params
     * @return
     */
    private String fetchRemote(HostInfo hostInfo, String path, Map<String, String> params) {
        String store = params.get(STORE_PARAM);
        String key = params.get(KEY_PARAM);
        String from = params.get(FROM_PARAM);
        String to = params.get(TO_PARAM);

        String url;

        if (from != null && to != null) {
            url = String.format(WebPath.URLs.URL_STORE_KEY_FROM_TO_FORMAT,
                    hostInfo.host(), hostInfo.port(), path, store, key, from, to);
        } else {
            url = String.format(WebPath.URLs.URL_STORE_KEY_FORMAT,
                    hostInfo.host(), hostInfo.port(), path, store, key);
        }

        String result = "";

        try {
            result = client.target(url).request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        return result;
    }
}
