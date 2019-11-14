package ahbun.lib;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TestLib {
    private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    public static <T> String convertToJson(T object) {
        return gson.toJson(object);
    }
}
