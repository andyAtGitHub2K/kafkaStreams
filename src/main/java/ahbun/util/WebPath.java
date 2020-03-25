package ahbun.util;

import lombok.Getter;

/***
 * WebPath provides access to web server paths that provide access to services and templates.
 */
public class WebPath {
    public static class URLs {
        @Getter public static final String GET_WINDOW_STORE_BY_KEY_AND_RANGE = "/window/:store/:key/:from/:to";
        @Getter public static final String GET_WINDOW_STORE_BY_KEY = "/window/:store/:key";
        @Getter public static final String GET_ALL_KV_STORES = "/kv/:store";
        @Getter public static final String GET_KV_FROM_STORE = "/kv/:store/:key";
        // internal URL
        @Getter public static final String GET_ALL_LOCAL_STORES = "/kv/:store/:local";
        @Getter public static final String GET_SESSION_STORE_BY_KEY = "/session/:store/:key";
        @Getter public static final String INTERACTIVE_QUERY_APP = "/iq";
        public static final String URL_STORE_KEY_FROM_TO_FORMAT = "http://%s:%d/%s/%s/%s/%s/%s";
        public static final String URL_STORE_KEY_FORMAT = "http://%s:%d/%s/%s/%s";
    }

    public static class HTMLs {
        @Getter public static final String INTERACTIVE_QUERY_APP = "interactiveQueriesApplication.html";
    }
}
