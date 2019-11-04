package ahbun.model;

import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.Assert.*;

public class CorrelatedPurchaseTest {
    Purchase pUatA;
    Purchase pUatB;

    @Before
    public void setup() throws ParseException {
        createP1();
    }

    @Test
    public void builder() {
    }

    @Test
    public void testBuilder() {
    }

    private void createP1() throws ParseException {
        Purchase.PurchaseBuilder builder = Purchase.builder();
        builder.creditCardNumber("abcde-1234-5678");
        builder.customerId("cid-123");
        builder.department("dept001");
        builder.employeeId("eid-001");
        builder.firstName("fname");
        builder.lastName("lname");
        builder.itemPurchased("pc");
        builder.price(100.00);
        builder.purchaseDate(LocalDateTime.now());
        builder.quantity(5);
        builder.storeId("store-01");
        builder.zipCode("12345");

        pUatA = builder.build();
    }

    private void createP2() throws ParseException {
        Purchase.PurchaseBuilder builder = Purchase.builder();
        builder.creditCardNumber("abcde-1234-5678");
        builder.customerId("cid-123");
        builder.department("dept001");
        builder.employeeId("eid-001");
        builder.firstName("fname");
        builder.lastName("lname");
        builder.itemPurchased("tv");
        builder.price(1000.00);
        /*Date d = new GregorianCalendar().

        builder.purchaseDate(new Date());
        builder.quantity(1);
        builder.storeId("store-01");
        builder.zipCode("12345");*/

        pUatA = builder.build();
    }
}