package ahbun.model;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class CurrencyTest {
    private Currency euro;
    private Currency usd;
    private Currency pounds;

    @Before
    public void setup() {
        euro = Currency.EUROS;
        usd = Currency.DOLLARS;
        pounds = Currency.POUNDS;
    }

    @Test
    public void checkCurrency() {
        Assert.assertTrue(Math.abs(Currency.EUROS.toUSD(100) - 100/1.09) < 0.0001);
        Assert.assertTrue(Math.abs(Currency.DOLLARS.toUSD(100) - 100) < 0.0001);
        Assert.assertTrue(Math.abs(Currency.POUNDS.toUSD(100) - 100/1.2929) < 0.0001);
    }
}