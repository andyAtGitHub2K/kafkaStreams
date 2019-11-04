package ahbun.model;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.time.LocalDateTime;

public class PurchasePatternTest {
    private PurchasePattern pp;
    private PurchasePattern.PatternBuilder patternbuilder;
    private LocalDateTime expectedDate = LocalDateTime.now();
    private int purchaseQty = 11;
    private double purchasePrice = 100.00;
    private double expectedAmout = purchaseQty* purchasePrice;
    private String expectedItem = "ooo";
    private String expectedZipcode = "12345";
    private Purchase purchase;
    @Before
    public void setup() throws Exception{
        Purchase.PurchaseBuilder builder = Purchase.builder();
        builder.creditCardNumber("12345678910");
        builder.customerId("cid-123");
        builder.department("dept001");
        builder.employeeId("eid-001");
        builder.firstName("fname");
        builder.lastName("lname");
        builder.itemPurchased(expectedItem);
        builder.price(purchasePrice);
        builder.purchaseDate(expectedDate);
        builder.quantity(purchaseQty);
        builder.storeId("store-01");
        builder.zipCode(expectedZipcode);
        purchase = builder.build();

        pp =  PurchasePattern.init(purchase).build();
    }

    @Test
    public void getZipCode() {
        assertEquals(expectedZipcode, pp.getZipCode());
    }

    @Test
    public void getItem() {

        assertEquals(expectedItem, pp.getItem());
    }

    @Test
    public void getDate() {
        assertEquals(expectedDate, pp.getDate());
    }

    @Test
    public void getAmount() {
        assertTrue((expectedAmout - pp.getAmount()) < 0.001);
    }
}