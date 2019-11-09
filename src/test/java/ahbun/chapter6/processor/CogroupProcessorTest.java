package ahbun.chapter6.processor;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import ahbun.model.ClickEvent;
import ahbun.model.StockTransaction;
import ahbun.util.Tuple;
import ahbun.lib.ObjectConverter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CogroupProcessorTest {
    private Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    String expectedJson;
    Instant testTime;
    String expectedToString;
    @Before
    public void setup() {
        expectedJson = "{\"x\":[{\"symbol\":\"abc\",\"clickInstance\":{\"seconds\":1573266245,\"nanos\":0},\"pageLink\":\"cde\"},{\"symbol\":\"abc\",\"clickInstance\":{\"seconds\":1573266250,\"nanos\":0},\"pageLink\":\"hij\"}],\"y\":[]}";
        testTime = Instant.ofEpochSecond(1573266245);
        expectedToString = "Tuple{x=[ClickEvent{symbol='abc', clickInstance=2019-11-09T02:24:05Z, pageLink='cde'}, ClickEvent{symbol='abc', clickInstance=2019-11-09T02:24:10Z, pageLink='hij'}], y=[]}";
    }

    @Test
    public void testTuple() {
        Tuple<List<ClickEvent>, List<StockTransaction>> tuple = new Tuple<>(new ArrayList<>(), new ArrayList<>());

        ClickEvent clickEvent = new ClickEvent("abc", testTime, "cde");
        tuple.getX().add(clickEvent);
        clickEvent = new ClickEvent("abc",
                testTime.plus(5, ChronoUnit.SECONDS),
                "hij");
        tuple.getX().add(clickEvent);

        String t = ObjectConverter.convertToJson(tuple);

        Type tupleType = new TypeToken<Tuple<List<ClickEvent>, List<StockTransaction>>>(){}.getType();
        Tuple<List<ClickEvent>, List<StockTransaction>> t2 = gson.fromJson(t, tupleType);
        Assert.assertTrue(expectedToString.equals(t2));
        Assert.assertEquals(expectedJson, t);
    }
}