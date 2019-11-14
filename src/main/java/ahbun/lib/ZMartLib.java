package ahbun.lib;

import ahbun.model.Purchase;
import ahbun.model.PurchasePattern;
import ahbun.model.Reward;

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.io.InputStream;
import java.util.Properties;
import java.util.function.Supplier;

public class ZMartLib {
    private static final String cafe = "cafe";
    private static final String electronics = "electronics";
    private static final String targetedEmployeeSuffix = "0000";
    public static ValueMapper<Purchase, Purchase> maskCreditCardMapper = p -> Purchase.builder(p).maskCreditCard().build();
    public static ValueMapper<Purchase, PurchasePattern> purchasePToPatternValueMapper
            = p ->  PurchasePattern.init(p).build();
    // remap key to purchase date
    public static KeyValueMapper<String, Purchase, Long> reMapKeyToPurchaseDate = (key, purchase) -> purchase.getpurchaseDateInEpochSeconds();
    public static ValueMapper<Purchase, Reward> purchaseRewardValueMapper = p-> Reward.builder(p).build();
    private static Supplier<Double> minimumPurchase = () -> {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream businessInputStream = classLoader.getResourceAsStream("chapter3/business.properties");
            Properties businessProperties = new Properties();
            businessProperties.load(businessInputStream);
            return Double.parseDouble(businessProperties.getOrDefault("minimum_purchase", "100.00").toString());
        } catch(Exception ex) {
            return 100.0;
        }
    };

    public static Predicate<String, Purchase> minimumPurchaseFilter = (k, p)->  p.getPrice() * p.getQuantity() > minimumPurchase.get();
    public static Predicate<String, Purchase> cafePredicate = (key, purchase) ->
            purchase.getDepartment().equalsIgnoreCase(cafe);
    public static Predicate<String, Purchase> electronicsPredicate = (key, purchase) ->
            purchase.getDepartment().equalsIgnoreCase(electronics);

    public static Predicate<String, Purchase> employeeIDFilter = (k,p) ->
            p.getEmployeeId().equalsIgnoreCase(targetedEmployeeSuffix);
}
