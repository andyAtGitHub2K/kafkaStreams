package ahbun.model;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class CorrelatedPurchase {
    private String customerID;
    private LocalDateTime purchaseDate1;
    private LocalDateTime purchaseDate2;
    private List<String> items;
    private double totalAmount;

    // static builder method to return the default builder
    public static CorrelatedPurchaseBuilder builder() { return new CorrelatedPurchaseBuilder(); }

    public static CorrelatedPurchaseBuilder builder(Purchase p1, Purchase p2)  {
        CorrelatedPurchaseBuilder builder = builder();
        // update builder value with correlatedPurchase
        String p1CID = p1 != null ? p1.getLastName() + "," + p1.getFirstName() : null;
        String p2CID = p2 != null ? p2.getLastName() + "," + p2.getFirstName(): null;
        builder.withCustomerID(p1CID!= null ? p1CID : p2CID);

        builder.withPurchaseDate1(p1 !=null ? p1.getPurchaseDate() : null);
        builder.withPurchaseDate2(p2 !=null ? p2.getPurchaseDate() : null);

        // sum items
        List<String> purchaseItems = new ArrayList<>();
        if (p1 != null && p1.getItemPurchased() != null) {
            purchaseItems.add(p1.getItemPurchased());
        }
        if (p2 != null && p2.getItemPurchased() != null) {
            purchaseItems.add(p2.getItemPurchased());
        }
        builder.withItems(purchaseItems);

        double totalPurchased = 0.0;
        if (p1 != null && p1.getPrice() > 0 && p1.getQuantity() > 0) {
            totalPurchased += p1.getPrice() * p1.getQuantity();
        }

        if (p2 != null && p2.getPrice() > 0 && p2.getQuantity() > 0) {
            totalPurchased += p2.getPrice() * p2.getQuantity();
        }

        builder.withTotalAmount(totalPurchased);
        return builder;
    }
    private CorrelatedPurchase(CorrelatedPurchaseBuilder builder) {
        this.customerID = builder.customerID;
        this.items = builder.items;
        this.purchaseDate1 = builder.purchaseDate1;
        this.purchaseDate2 = builder.purchaseDate2;
        this.totalAmount = builder.totalAmount;
    }
    private CorrelatedPurchase(){}

    public String getCustomerID() {
        return customerID;
    }

    public LocalDateTime getPurchaseDate1() {
        return purchaseDate1;
    }

    public LocalDateTime getPurchaseDate2() {
        return purchaseDate2;
    }

    public List<String> getItems() {
        return items;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (String s : items) {
            builder.append(s);
            builder.append(", ");
        }

        return String.format(">> id: %s, items: %s\n date1: %s, date2: %s\n",
                customerID, builder.toString(),
                purchaseDate1.toString(), purchaseDate2.toString());
    }
    public static final class CorrelatedPurchaseBuilder {
        private String customerID;
        private LocalDateTime purchaseDate1;
        private LocalDateTime purchaseDate2;
        private List<String> items;
        private double totalAmount;

        public CorrelatedPurchase build() { return new CorrelatedPurchase(this);}

        private CorrelatedPurchaseBuilder(){}

        public CorrelatedPurchaseBuilder withCustomerID(String customerID) {
            this.customerID = customerID;
            return this;
        }

        public CorrelatedPurchaseBuilder withPurchaseDate1(LocalDateTime purchaseDate1) {
            this.purchaseDate1 = purchaseDate1;
            return this;
        }

        public CorrelatedPurchaseBuilder withPurchaseDate2(LocalDateTime purchaseDate2) {
            this.purchaseDate2 = purchaseDate2;
            return this;
        }

        public CorrelatedPurchaseBuilder withItems(List<String> items) {
            this.items = items;
            return this;
        }

        public CorrelatedPurchaseBuilder withTotalAmount(double totalAmount) {
            this.totalAmount = totalAmount;
            return this;
        }
    }
}
