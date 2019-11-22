package ahbun.model;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import static org.junit.Assert.*;

public class TransactionSummaryTest {
    private TransactionSummary summary;
    private TransactionSummary.TransactionSummaryBuilder builder;
    private StockTransaction.STransactionBuilder txBuilder;
    private StockTransaction transaction;
    private ZoneId ZONE_ID = ZoneId.of("America/Los_Angeles");
    private String symbol;
    private int quantity;
    private LocalDateTime localDateTime;
    private double price;
    private String cid;
    private LocalDateTime purchaseTS;
    private String key;
    @Before
    public void setup() {
        cid = "abc";
        quantity = 100;
        price = 200.0;
        symbol = "HIJ";
        purchaseTS = LocalDateTime.now();
        Date localDate = new Date(purchaseTS.toInstant(ZONE_ID.getRules().getOffset(purchaseTS)).toEpochMilli());
        key = cid + "-" + symbol;

        // initialize transaction
        txBuilder = StockTransaction.builder();
        txBuilder.symbol(symbol)
                .transactionTimestamp(localDate)
                .shares(quantity)
                .sharePrice(price)
                .sector("manufacturing")
                .industry("transportation")
                .customerId(cid)
                .purchase(true);

        transaction = txBuilder.build();

        builder = TransactionSummary.builder(transaction);
        summary = builder.build();
    }

    @Test
    public void builder() {
//        System.out.println(summary);
//        Assert.assertEquals(cid, summary.getCustomerId());
//        Assert.assertTrue(quantity == summary.getQuantity());
//        Assert.assertTrue(Math.abs(price - summary.getSharePrice()) < 0.0001);
//        Assert.assertEquals(symbol, summary.getSymbol());
//        Assert.assertEquals(purchaseTS, summary.getPurchaseDateTime());
//        Assert.assertEquals(key, TransactionSummary.TransactionSummaryBuilder.getKeyFrom(transaction));
    }
}