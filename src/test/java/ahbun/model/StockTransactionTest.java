package ahbun.model;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

public class StockTransactionTest {
    private StockTransaction.STransactionBuilder builder;
    private StockTransaction transaction;
    private ZoneId ZONE_ID = ZoneId.of("America/Los_Angeles");
    private LocalDateTime now = LocalDateTime.now();
    private Date localDate =  new Date(now.toInstant(ZONE_ID.getRules().getOffset(now)).toEpochMilli());
    @Before
    public void setup() {
        builder = StockTransaction.builder();
        builder.symbol("abc")
                .transactionTimestamp(localDate)
                .shares(100)
                .sharePrice(100.01)
                .sector("manufacturing")
                .industry("transportation")
                .customerId("cid-001")
                .purchase(true);

        transaction = builder.build();
    }


    @Test
    public void builder() {
        StockTransaction.STransactionBuilder myBuilder = StockTransaction.builder(transaction);
        StockTransaction myTx = myBuilder.build();
        Assert.assertEquals(transaction, myTx);
        Assert.assertEquals("abc", myTx.getSymbol());
        Assert.assertEquals(localDate, myTx.getTransactionTimestamp());
        Assert.assertEquals(100, myTx.getShares());
        Assert.assertTrue(Math.abs(100.01 - myTx.getSharePrice()) < 0.001);
        Assert.assertEquals("manufacturing", myTx.getSector());
        Assert.assertEquals("transportation", myTx.getIndustry());
        Assert.assertEquals("cid-001", myTx.getCustomerId());
        Assert.assertTrue(myTx.purchase);
        ZoneId zoneId = ZoneId.of("America/Los_Angeles");

        DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS-0400")
                .withLocale(Locale.US)
                .withZone(ZoneId.systemDefault());

        String timestamp = myTx.getTransactionTimestamp().toString();//  .format(formatter);

        System.out.println(formatter.format(myTx.getTransactionTimestamp().toInstant()));
        System.out.println(timestamp);
    }
}