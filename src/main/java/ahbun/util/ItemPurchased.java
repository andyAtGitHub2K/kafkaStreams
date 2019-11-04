package ahbun.util;

import ahbun.model.Purchase;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ItemPurchased {
    private  String department;
    private  String item;
    private  int quantity;
    private  double unitPrice;
    private LocalDateTime purchasedDate;

    @Override
    public String toString() {
        /*
         * \"itemPurchased\":\"tvb\",\"department\":\"electronics\",
         * "employeeId":"emp-01001","quantity":2,"price":120.20,
         * \"purchaseDate\":\"2019-09-22T03:38:18.309Z\","zipCode":"23456"}
         */
        return String.format("\"itemPurchased\":\"" + item +
                "\",\"department\":\"" + department + "\",\"purchaseDate\":\"" +
                purchasedDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "\",\"quantity\":" + quantity + ",\"price\":" + unitPrice);
    }

    public static ItemPurchasedBuilder builder() { return new ItemPurchasedBuilder();}

    public static ItemPurchasedBuilder builder(final Purchase purchased) {
        ItemPurchasedBuilder builder = builder();
        builder.department = purchased.getDepartment();
        builder.item = purchased.getItemPurchased();
        builder.purchasedDate =  purchased.getPurchaseDate();
        builder.quantity = purchased.getQuantity();
        builder.unitPrice = purchased.getPrice();
        return builder;
    }
    private ItemPurchased(){}
    private ItemPurchased(ItemPurchasedBuilder builder) {
        department = builder.department;
        item = builder.item;
        purchasedDate = builder.purchasedDate;
        quantity = builder.quantity;
        unitPrice = builder.unitPrice;
    }

    public String getDepartment() {
        return department;
    }

    public String getItem() {
        return item;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getUnitPrice() {
        return unitPrice;
    }

    public LocalDateTime getPurchasedDate() {
        return purchasedDate;
    }

    public static final class ItemPurchasedBuilder {
        private String department;
        private String item;
        private int quantity;
        private double unitPrice;
        private LocalDateTime purchasedDate;
        private static String datePattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX";
        private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(datePattern);

        public String getDepartment() {
            return department;
        }

        public String getItem() {
            return item;
        }

        public int getQuantity() {
            return quantity;
        }

        public double getUnitPrice() {
            return unitPrice;
        }

        public LocalDateTime getPurchasedDate() {
            return purchasedDate;
        }

        @Override
        public String toString() {
            /*
             * \"itemPurchased\":\"tvb\",\"department\":\"electronics\",
             * "employeeId":"emp-01001","quantity":2,"price":120.20,
             * \"purchaseDate\":\"2019-09-22T03:38:18.309Z\","zipCode":"23456"}
             */
            return String.format("\"itemPurchased\":\"" + item +
                    "\",\"department\":\"" + department + "\",\"purchaseDate\":\"" +
                    purchasedDate + "\"");
        }

        public ItemPurchased build() { return new ItemPurchased(this); }

        public ItemPurchasedBuilder setDepartment(String department) {
            this.department = department;
            return this;
        }

        public ItemPurchasedBuilder setItem(String item) {
            this.item = item;
            return this;
        }

        public ItemPurchasedBuilder setQuantity(int quantity) {
            this.quantity = quantity;
            return this;
        }

        public ItemPurchasedBuilder setUnitPrice(double unitPrice) {
            this.unitPrice = unitPrice;
            return this;
        }

        public ItemPurchasedBuilder setPurchasedDate(LocalDateTime purchasedDate) {
            this.purchasedDate = purchasedDate;
            return this;
        }

        public static void setDatePattern(String datePattern) {
            ItemPurchasedBuilder.datePattern = datePattern;
        }

        // not allow to instantiate except through this package
        private ItemPurchasedBuilder(){}
    }
}
