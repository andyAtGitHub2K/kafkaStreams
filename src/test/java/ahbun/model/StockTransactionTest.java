package ahbun.model;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.junit.Assert.*;

public class StockTransactionTest {
    private StockTransaction.STransactionBuilder builder;
    private StockTransaction transaction;
    private LocalDateTime now = LocalDateTime.now();
    @Before
    public void setup() {
        builder = StockTransaction.builder();
        builder.symbol("abc")
                .transactionTimestamp(now)
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
        Assert.assertEquals(now, myTx.getTransactionTimestamp());
        Assert.assertEquals(100, myTx.getShares());
        Assert.assertTrue(Math.abs(100.01 - myTx.getSharePrice()) < 0.001);
        Assert.assertEquals("manufacturing", myTx.getSector());
        Assert.assertEquals("transportation", myTx.getIndustry());
        Assert.assertEquals("cid-001", myTx.getCustomerId());
        Assert.assertTrue(myTx.purchase);
    }
}