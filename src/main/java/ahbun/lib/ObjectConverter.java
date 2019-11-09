package ahbun.lib;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.List;

public class ObjectConverter {
    private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    public static  <T> List<String> convertToJson(List<T> generatedData) {
        List<String> jsonString = new ArrayList<>();

        generatedData.forEach(s -> {
            jsonString.add(convertToJson(s));
        });

        return jsonString;
    }
    public static  <T> String convertToJson(T object) {
        return gson.toJson(object);
    }
}
