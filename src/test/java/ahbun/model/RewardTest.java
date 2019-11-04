package ahbun.model;

import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Date;

import static org.junit.Assert.*;

public class RewardTest {
    private Reward.RewardBuilder rewardBuilder;
    private Reward reward;
    private Purchase purchase;
    private double unitPrice = 100.00;
    private int qty = 11;
    private LocalDateTime expectedDate = LocalDateTime.now();
    @Before
    public void setup() throws Exception {
        Purchase.PurchaseBuilder builder = Purchase.builder();
        builder.customerId("cid-123");
        builder.department("dept001");
        builder.employeeId("eid-001");
        builder.firstName("fname");
        builder.lastName("lname");
        builder.itemPurchased("pc");
        builder.price(unitPrice);
        builder.purchaseDate(expectedDate);
        builder.quantity(qty);
        builder.storeId("store-01");
        builder.zipCode("12345");

        purchase = builder.build();
        reward = Reward.builder(purchase).build();
    }

    @Test
    public void builder() {
        assertTrue(reward.getCustumerID().equals("cid-123"));
        assertTrue(reward.getPurchaseTotal() - unitPrice * qty < 0.001 ||
                reward.getPurchaseTotal() - unitPrice * qty >= -0.001 );
        assertTrue(reward.getTotalPurchasePoint() == (int) reward.getPurchaseTotal()) ;
    }
}