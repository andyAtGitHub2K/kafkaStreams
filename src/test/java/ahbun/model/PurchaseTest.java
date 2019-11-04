package ahbun.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.time.LocalDateTime;

public class PurchaseTest {
    Purchase pUat;
    String unmaskedCCN = "abcde-1234-5678";
    String maskedCCN = "xxxx-xxxx-xxxx-5678";
    @Before
    public void setUp() throws Exception {
        Purchase.PurchaseBuilder builder = Purchase.builder();
        builder.creditCardNumber(unmaskedCCN);
        builder.customerId("cid-123");
        builder.department("dept001");
        builder.employeeId("eid-001");
        builder.firstName("fname");
        builder.lastName("lname");
        builder.itemPurchased("pc");
        builder.price(100.00);
        builder.purchaseDate(LocalDateTime.now());
        builder.quantity(11);
        builder.storeId("store-01");
        builder.zipCode("12345");

        pUat = builder.build();
    }

    @Test
    public void maskCreditCard() {
        assertNotNull(pUat);
        assertEquals(unmaskedCCN, pUat.getCreditCardNumber());

        // instantiate the builder that wrap the Purchase to
        // create a new Purchase with modified CCN.
        Purchase.PurchaseBuilder builder = Purchase.builder(pUat);
        assertEquals(unmaskedCCN, builder.build().getCreditCardNumber());
        System.out.println(pUat.getCreditCardNumber());
        System.out.println(builder.build().getCreditCardNumber());


        assertEquals(pUat.getCreditCardNumber(), builder.build().getCreditCardNumber());
        assertEquals(maskedCCN, builder.maskCreditCard().build().getCreditCardNumber());


        assertNotEquals(pUat.getCreditCardNumber(), builder.maskCreditCard().build().getCreditCardNumber());
        System.out.println(builder.maskCreditCard().build().getCreditCardNumber());
        System.out.println(pUat.getCreditCardNumber());
    }

    @Test
    public void jsonConvertion() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        String s = gson.toJson(pUat);
        System.out.println(s);
    }
}