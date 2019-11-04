package ahbun.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
//import java.util.Date;
import java.time.ZoneId;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Purchase is a model of the purchase transaction
 */
public class Purchase {
    private static ZoneId zoneId = TimeZone.getDefault().toZoneId();
    private String firstName;
    private String lastName;
    private String customerId;
    private String creditCardNumber;
    private String itemPurchased;
    private String department;
    private String employeeId;
    private int quantity;
    private double price;
    private LocalDateTime purchaseDate;
    private String zipCode;
    private String storeId;

    // encaptualte PurchaseBuilder constructor. Only Purchase package can
    // get PurchaseBuilder
    public static PurchaseBuilder builder() { return new PurchaseBuilder(); }

    /***
     * Instantiate the PurchaseBuilder with a copy of Purchase item.
     * @param purchase
     * @return
     */
    public static PurchaseBuilder builder(final Purchase purchase) {
        PurchaseBuilder purchaseBuilder = builder();
        purchaseBuilder.creditCardNumber = purchase.creditCardNumber;
        purchaseBuilder.customerId = purchase.customerId;
        purchaseBuilder.department = purchase.department;
        purchaseBuilder.employeeId = purchase.employeeId;
        purchaseBuilder.firstName = purchase.firstName;
        purchaseBuilder.lastName = purchase.lastName;
        purchaseBuilder.itemPurchased = purchase.itemPurchased;
        purchaseBuilder.price = purchase.price;
        purchaseBuilder.purchaseDate = purchase.purchaseDate;
        purchaseBuilder.quantity = purchase.quantity;
        purchaseBuilder.storeId = purchase.storeId;
        purchaseBuilder.zipCode = purchase.zipCode;
        return purchaseBuilder;
    }

    private static LocalDateTime convertStrToDate(final String dateInStr) throws ParseException {
        return  LocalDateTime.parse(dateInStr);
        //SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        //return d.parse(dateInStr);
    }


    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getCreditCardNumber() {
        return creditCardNumber;
    }

    public String getItemPurchased() {
        return itemPurchased;
    }

    public String getDepartment() {
        return department;
    }

    public String getEmployeeId() {
        return employeeId;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    public LocalDateTime getPurchaseDate() {
        return purchaseDate;
    }

    public long getpurchaseDateInEpochSeconds() {
        return purchaseDate.toEpochSecond(zoneId.getRules().getOffset(purchaseDate));
    }

    public String getZipCode() {
        return zipCode;
    }

    public String getStoreId() {
        return storeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Purchase purchase = (Purchase) o;
        return quantity == purchase.quantity &&
                Double.compare(purchase.price, price) == 0 &&
                Objects.equals(firstName, purchase.firstName) &&
                Objects.equals(lastName, purchase.lastName) &&
                Objects.equals(customerId, purchase.customerId) &&
                Objects.equals(creditCardNumber, purchase.creditCardNumber) &&
                Objects.equals(itemPurchased, purchase.itemPurchased) &&
                Objects.equals(department, purchase.department) &&
                Objects.equals(employeeId, purchase.employeeId) &&
                Objects.equals(purchaseDate, purchase.purchaseDate) &&
                Objects.equals(zipCode, purchase.zipCode) &&
                Objects.equals(storeId, purchase.storeId);
    }

    @Override
    public int hashCode() {
        // firstName, lastName, customerId, creditCardNumber,
        // itemPurchased, department, employeeId, quantity, price,
        // purchaseDate, zipCode, storeId
        int hash;
        long tempPrice;

        hash = firstName != null ? firstName.hashCode() : 0;
        hash = 29 + hash + (lastName != null ? lastName.hashCode() : 0);
        hash = 29 + hash + (customerId != null ? customerId.hashCode() : 0);
        hash = 29 + hash + (creditCardNumber != null ? creditCardNumber.hashCode() : 0);
        hash = 29 + hash + (itemPurchased != null ? itemPurchased.hashCode() : 0);
        hash = 29 + hash + (department != null ? department.hashCode() : 0);
        hash = 29 + hash + (employeeId != null ? employeeId.hashCode() : 0);
        hash = 29 + hash + quantity;
        tempPrice = Double.doubleToLongBits(price);
        hash = 29 + hash + (int)(tempPrice ^ tempPrice >>> 32);
        hash = 29 + hash + (purchaseDate != null ? purchaseDate.hashCode() : 0);
        hash = 29 + hash + (zipCode != null ? zipCode.hashCode() : 0);
        hash = 29 + hash + (storeId != null ? storeId.hashCode() : 0);

        return hash;
    }

    @Override
    public String toString() {
        return "Purchase{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", customerId='" + customerId + '\'' +
                ", creditCardNumber='" + creditCardNumber + '\'' +
                ", itemPurchased='" + itemPurchased + '\'' +
                ", department='" + department + '\'' +
                ", employeeId='" + employeeId + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                ", purchaseDate=" + purchaseDate +
                ", zipCode='" + zipCode + '\'' +
                ", storeId='" + storeId + '\'' +
                '}';
    }

    private Purchase(PurchaseBuilder builder) {
        firstName = builder.firstName;
        lastName = builder.lastName;
        customerId = builder.customerId;
        creditCardNumber = builder.creditCardNumber;
        itemPurchased = builder.itemPurchased;
        quantity = builder.quantity;
        price = builder.price;
        purchaseDate = builder.purchaseDate;
        zipCode = builder.zipCode;
        employeeId = builder.employeeId;
        department = builder.department;
        storeId = builder.storeId;
    }

    // Use a Builder to make copy of Purchase and provide a maskCreditCard
    public static final class PurchaseBuilder {
        private String firstName;
        private String lastName;
        private String customerId;
        private String creditCardNumber;
        private String itemPurchased;
        private String department;
        private String employeeId;
        private int quantity;
        private double price;
        private LocalDateTime purchaseDate;
        private String zipCode;
        private String storeId;
        
        public Purchase build() {
            return new Purchase(this);
        }

        public PurchaseBuilder maskCreditCard() {
            creditCardNumber = creditCardMask + creditCardNumber.substring(creditCardNumber.length() - 4);
            return this;
        }

        public PurchaseBuilder firstName(String firstName) {
            this.firstName = firstName;
            return this;
        }

        public PurchaseBuilder lastName(String lastName) {
            this.lastName = lastName;
            return this;
        }

        public PurchaseBuilder customerId(String customerId) {
            this.customerId = customerId;
            return this;
        }

        public PurchaseBuilder creditCardNumber(String creditCardNumber) {
            this.creditCardNumber = creditCardNumber;
            return this;
        }

        public PurchaseBuilder itemPurchased(String itemPurchased) {
            this.itemPurchased = itemPurchased;
            return this;
        }

        public PurchaseBuilder department(String department) {
            this.department = department;
            return this;
        }

        public PurchaseBuilder employeeId(String employeeId) {
            this.employeeId = employeeId;
            return this;
        }

        public PurchaseBuilder quantity(int quantity) {
            this.quantity = quantity;
            return this;
        }

        public PurchaseBuilder price(double price) {
            this.price = price;
            return this;
        }

        public PurchaseBuilder purchaseDate(LocalDateTime purchaseDate) throws ParseException {
            this.purchaseDate = purchaseDate;// convertStrToDate(purchaseDate);
            return this;
        }

        public PurchaseBuilder zipCode(String zipCode) {
            this.zipCode = zipCode;
            return this;
        }

        public PurchaseBuilder storeId(String storeId) {
            this.storeId = storeId;
            return this;
        }

        private static final String creditCardMask = "xxxx-xxxx-xxxx-";

        private PurchaseBuilder() {}
    }
}
