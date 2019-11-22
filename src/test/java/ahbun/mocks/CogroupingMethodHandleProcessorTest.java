package ahbun.mocks;

import ahbun.chapter6.processor.CogroupProcessor;
import ahbun.model.ClickEvent;
import ahbun.model.StockTransaction;
import ahbun.util.Tuple;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.junit.Assert;
import org.junit.Test;

import java.time.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.apache.kafka.streams.processor.PunctuationType.STREAM_TIME;
import static org.mockito.Mockito.*;

public class CogroupingMethodHandleProcessorTest {
    private ProcessorContext processorContext = mock(ProcessorContext.class);
    private String stateStoreName = "mystore";
    private CogroupProcessor processor = new CogroupProcessor(stateStoreName, Duration.ofMillis(5000));
    private MockKeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> mockKVStore =
            new MockKeyValueStore<>();
    private ZoneId ZONE_ID = ZoneId.of("America/Los_Angeles");

    @Test
    public void testInit() {
        processor.init(processorContext);
        // verify getStateStore is called with stateStoreName
        verify(processorContext).getStateStore(stateStoreName);
        // verify the arguments of schedule method is called with the correct arguments when init is called
        verify(processorContext).schedule(eq(Duration.ofMillis(5000)), eq(STREAM_TIME), isA(Punctuator.class));
    }

    @Test
    public void testProcess() {
        ClickEvent clickEventToProcess = new ClickEvent("A", Instant.ofEpochSecond(500), "link A");
        LocalDateTime date = LocalDateTime.of(2019,11,10,10,20);
        Date localDate = new Date(date.toInstant(ZONE_ID.getRules().getOffset(date)).toEpochMilli());
        StockTransaction stockTransactionToProcess = getTransaction(localDate);
        List<ClickEvent> clickEventList = new ArrayList<>();
        List<StockTransaction> stockTransactionList = new ArrayList<>();
        clickEventList.add(clickEventToProcess);
        stockTransactionList.add(stockTransactionToProcess);

        when(processorContext.getStateStore(stateStoreName)).thenReturn(mockKVStore);
        processor.init(processorContext);
        processor.process("A", new Tuple<>(clickEventToProcess, stockTransactionToProcess));

        Tuple<List<ClickEvent>, List<StockTransaction>> tupleA = mockKVStore.get("A");
        Assert.assertTrue(tupleA.getX().size() == 1);
        ClickEvent clickEventFromStore = tupleA.getX().get(0);
        Assert.assertEquals(clickEventToProcess, clickEventFromStore);

        Assert.assertTrue(tupleA.getY().size() == 1);
        StockTransaction stockTransactionFromStore = tupleA.getY().get(0);
        Assert.assertEquals(stockTransactionToProcess, stockTransactionFromStore);
    }

    @Test
    public void testPunctuate() {
        ClickEvent clickEventToProcess = new ClickEvent("A", Instant.ofEpochSecond(500), "link A");
        LocalDateTime date = LocalDateTime.of(2019,11,10,10,20);
        Date localDate = new Date(date.toInstant(ZONE_ID.getRules().getOffset(date)).toEpochMilli());
        StockTransaction stockTransactionToProcess = getTransaction(localDate);
        List<ClickEvent> clickEventList = new ArrayList<>();
        List<StockTransaction> stockTransactionList = new ArrayList<>();
        clickEventList.add(clickEventToProcess);
        stockTransactionList.add(stockTransactionToProcess);

        when(processorContext.getStateStore(stateStoreName)).thenReturn(mockKVStore);
        processor.init(processorContext);
        processor.process("A", new Tuple<>(clickEventToProcess, null));
        processor.process("A", new Tuple<>(null, stockTransactionToProcess));
        Tuple<List<ClickEvent>, List<StockTransaction>> tupleA = mockKVStore.get("A");
        List<ClickEvent> clickEventListA = new ArrayList<>(tupleA.getX());
        List<StockTransaction> stockTransactionListA = new ArrayList<>(tupleA.getY());
        Assert.assertEquals(1, clickEventListA.size());
        Assert.assertEquals(1, stockTransactionListA.size());
        System.out.println(clickEventListA.get(0));
        System.out.println(stockTransactionListA.get(0));
        processor.getPunctuator().punctuate(12345678L);
        verify(processorContext).forward("A", new Tuple<>(clickEventListA, stockTransactionListA));
        Assert.assertEquals(0, tupleA.getX().size());
        Assert.assertEquals(0, tupleA.getY().size());
    }

    private StockTransaction getTransaction(Date localDateTime) {
        StockTransaction.STransactionBuilder builder = StockTransaction.builder();
        builder.symbol("abc")
                .transactionTimestamp(localDateTime)
                .shares(100)
                .sharePrice(100.01)
                .sector("manufacturing")
                .industry("transportation")
                .customerId("cid-001")
                .purchase(true);

        return builder.build();
    }
}
