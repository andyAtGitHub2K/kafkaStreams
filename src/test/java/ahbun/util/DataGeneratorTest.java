package ahbun.util;

import ahbun.model.BeerDistribution;
import ahbun.model.Purchase;
import ahbun.model.StockTickerData;
import ahbun.model.StockTransaction;
import com.github.javafaker.Faker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class DataGeneratorTest {
    private Faker faker;
    private DataGenerator generator;
    private int nameListSize;
    @Before
    public void setup() {
        faker = new Faker();
        generator = new DataGenerator();
        nameListSize = 5;
    }

    @Test
    public void testFakeName() {
        String name = faker.name().fullName();
        String firstName = faker.name().firstName();
        String lastName = faker.name().lastName();

        System.out.printf("%s %s %s", name, firstName, lastName);
    }

    @Test
    public void testCreateNames() {
        List<String> nameList = generator.createNames(nameListSize);
        Assert.assertTrue(nameList.size() == nameListSize);
        Iterator<String> it = nameList.listIterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }

    @Test
    public void testRandomNameFromList() {
        List<String> nameList = generator.createNames(nameListSize);
        for (int i = 0; i < 10; i++) {
            System.out.println(generator.randomItemFromList(nameList));
        }
    }

    @Test
    public void testItemPurchased() {
       ItemPurchased itemPurchased = generator.createItemPurchased();
       System.out.println(itemPurchased.toString());

    }

    @Test
    public void testCreateCustomer() {
        Customer customer = generator.createCustomer();
        System.out.println(customer.toString());
    }

    @Test
    public void testCustomerList() {
        List<Customer> customerList = generator.createCustomerList(nameListSize);
        customerList.stream().forEach(c -> {
            System.out.println(c.toString() + "\n");
        });
    }

    @Test
    public void createEmployee() {
        Employee employee = generator.createEmployee();
        Assert.assertTrue(employee.getEmployeeId().contains("eid-"));
        System.out.println(employee.toString());
    }

    @Test
    public void createEmployeeList() {
        List<Employee> employeeList = generator.createEmployeeList(5);
        Assert.assertTrue(employeeList.size() == 5);
        employeeList.stream().forEach(c -> {
            System.out.println(c.toString() + "\n");
        });
    }

    @Test
    public void createLocalDateTime() {
        Supplier<Date> timestampGenerator = () -> faker.date().past(15, TimeUnit.MINUTES, new Date());
        Date da = timestampGenerator.get();
        Date db = timestampGenerator.get();

        Date before;
        Date after;

        if (da.before(db)) {
            before = da;
            after = db;
        } else {
            before = db;
            after = da;
        }
        ZoneId zoneId = TimeZone.getDefault().toZoneId();
        ZoneOffset offset = Instant.now().atZone(zoneId).getOffset();

        Date d = faker.date().between(before, after);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(d.toInstant(), zoneId);
        System.out.println(localDateTime.toString());
    }
//
//    @Test
//    public void testEmployeeList() {
//        List<DataGenerator.Employee> employeeList = generator.createEmployee(5);
//        employeeList.stream().forEach(c -> {
//            Assert.assertTrue(c.getEmployeeId().length() == 8);
//            Assert.assertTrue(c.getZipCode().length() >= 5);
//            System.out.println(c.toString() + "\n");
//        });
//    }
//
//    @Test
//    public void testItemPurchased() {
//        DataGenerator.ItemPurchased item = generator.makeItemPurchased();
//        System.out.println(item.toString());
//    }
//
    @Test
    public void testCreatePurchase() {
        try {
            PurchaseSimiluationRecipe recipe = new PurchaseSimiluationRecipe(3, 6, 5);
            List<Purchase> purchaseList = DataGenerator.createPurchase(recipe);
            purchaseList.stream().forEach(p -> System.out.println(p.toString()));
        } catch (Exception ex) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testCreateTickerData() {
        StockTickerData sd;
        for (int i = 0; i < 5; i++) {
            sd = DataGenerator.createStockTickerData();
            System.out.println(sd.toString());
        }
    }

    @Test
    public void testCreateStockTx() {
        String[] industries = new String[]{"a", "b", "c"};
        List<StockTransaction> txList = DataGenerator.makeStockTx(5, industries);

        txList.stream().forEach(s -> System.out.println(s.toString()));
    }

    @Test
    public void makeStockTxInWindow() {
        String[] cid = {"100", "200", "300"};
        String[] industries = {"food", "toy", "table", "book"};
        List<StockTransaction> stockTransactions = DataGenerator.makeStockTxInTradingWindow(
                4, cid , industries, LocalDateTime.now(),40);

        for (StockTransaction tx :stockTransactions) {
            System.out.println(tx);
        }
    }

    @Test
    public void testMakeStockTxWithinWindow() {
        int[] windowSize = {20, 40, 80};
        int[] gap = {5, 10, 15};
        String[] industries = new String[]{"a", "b", "c"};
        List<StockTransaction> stockTransactionList =
                DataGenerator.makeStockTxWithinWindow(
                        3,5,2,5,windowSize,gap, industries);

        for (StockTransaction tx: stockTransactionList) {
            System.out.printf("id: %s (%s) [%s]\n",
                    tx.getCustomerId(),tx.getSymbol(), tx.getTransactionTimestamp());
        }
    }

    @Test
    public void testBeerDistribution() {
       List<BeerDistribution> distributions =  DataGenerator.makeBeerDistribution(10);
       for (BeerDistribution beerDistribution: distributions) {
           System.out.println(beerDistribution.toString());
       }
    }
}