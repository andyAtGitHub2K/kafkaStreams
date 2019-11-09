package ahbun.util;

import ahbun.model.*;
import ahbun.model.Currency;
import com.github.javafaker.DateAndTime;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class DataGenerator {
    private static Faker faker = new Faker();
    private static final String[] departmentList = {"book", "beer", "food"};
    private static final String[] symbolList = {"ABC", "XYZ", "OOO", "FIG"};
    private static final String[] industryList = {"book", "food", "game", "clothing"};
    public final static String[] BEER_TYPE = {"white", "dark", "simple", "draft"};
    private static ZoneId zoneId = TimeZone.getDefault().toZoneId();
    private static ZoneOffset offset = Instant.now().atZone(zoneId).getOffset();
    private static String datePattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX";//"yyyy-MM-dd HH:mm:ss.SSSZ";
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(datePattern);
    private static Supplier<Date> timestampGenerator = () -> faker.date().past(45, TimeUnit.MINUTES, new Date());
    private static Logger logger = LoggerFactory.getLogger(DataGenerator.class);
    private static Function<String, String> symbolSupplier = (industry) -> industry + "_" + faker.number().numberBetween(1,5);

    /***
     * {"firstName":"hi-b","lastName-b":"lala-b",
     * "customerId":"cid-002",
     * "creditCardNumber":"asdcd234222234",
     * "itemPurchased":"tvb","department":"electronics",
     * "employeeId":"emp-01001","quantity":2,"price":120.20,
     * "purchaseDate":"2019-09-22T03:38:18.309Z","zipCode":"23456"}
     * @param size
     * @return
     */
    public List<String> createNames(int size) {
        ArrayList<String> names = new ArrayList<>();
        for (int i=0; i < size; i++) {
            names.add(String.format("%s,%s",
                    faker.name().firstName(),
                    faker.name().lastName()));
        }

        return names;
    }

    public Customer createCustomer() {
        Customer.CustomerBuilder builder = Customer.builder();
        builder.setLastName(faker.name().lastName());
        builder.setFirstName(faker.name().firstName());
        builder.setCreditCardNumber(faker.business().creditCardNumber());
        builder.setCustomerID(faker.idNumber().valid());
        return builder.build();
    }

    private static Customer createCustomer(Customer.CustomerBuilder builder) {
        builder.setLastName(faker.name().lastName());
        builder.setFirstName(faker.name().firstName());
        builder.setCreditCardNumber(faker.business().creditCardNumber());
        builder.setCustomerID(faker.idNumber().valid());
        return builder.build();
    }

    public static List<Customer> createCustomerList(int size) {
        ArrayList<Customer> customerArrayList = new ArrayList<>();
        Customer.CustomerBuilder builder = Customer.builder();
        for (int i = 0; i < size; i++) {
            customerArrayList.add(createCustomer(builder));
        }

        return customerArrayList;
    }

    public static Employee createEmployee() {
        Employee.EmployeeBuilder builder = Employee.builder();
        builder.setEmployeeId(generateEmployeeId());
        builder.setZipCode(faker.address().zipCode());
        return builder.build();
    }

    public static List<Employee> createEmployeeList(int size) {
        ArrayList<Employee> employeeArrayList = new ArrayList<>();
        Employee.EmployeeBuilder builder = Employee.builder();
        for (int i = 0; i < size; i++) {
            employeeArrayList.add(createEmployee());
        }

        return employeeArrayList;
    }

    private static String generateEmployeeId() {
        return "eid-" +faker.idNumber().valid().substring(7,11);
    }

    /***
     * createPurchase randomly generate a Purchase
     * @return Purchase{@link ahbun.model.Purchase}
     */
    public static List<Purchase> createPurchase(PurchaseSimiluationRecipe recipe) throws ParseException {
        List<Customer> customerList = createCustomerList(recipe.getMaxCustomers());
        List<Employee> employeeList = createEmployeeList(recipe.getMaxEmployees());
        List<Purchase> purchaseList = new ArrayList<>();
        Customer customer;
        Employee employee;
        ItemPurchased itemPurchased;
        Purchase purchase;
        for (int i = 0; i < recipe.getIterations(); i++) {
            customer = customerList.get(faker.number().numberBetween(0, recipe.getMaxCustomers() - 1));
            employee = employeeList.get(faker.number().numberBetween(0, recipe.getMaxEmployees() - 1));
            itemPurchased = createItemPurchased();
            purchase = Purchase.builder()
                    .purchaseDate(itemPurchased.getPurchasedDate())
                    .creditCardNumber(customer.getCreditCardNumber())
                    .customerId(customer.getCustomerID())
                    .department(itemPurchased.getDepartment())
                    .employeeId(employee.getEmployeeId())
                    .firstName(customer.getFirstName())
                    .itemPurchased(itemPurchased.getItem())
                    .lastName(customer.getLastName())
                    .price(itemPurchased.getUnitPrice())
                    .quantity(itemPurchased.getQuantity())
                    .storeId(employee.getZipCode())
                    .zipCode(employee.getZipCode())
                    .build();
            purchaseList.add(purchase);
        }
        return purchaseList;
    }

    public String randomItemFromList(List<String> nameList) {
        return nameList.get((int) (Math.random() * 100) % nameList.size());
    }

    private static String randomDepartment() {
        return departmentList[faker.number().numberBetween(0, departmentList.length - 1)] ;
    }

    public static ItemPurchased createItemPurchased() {
        ItemPurchased.ItemPurchasedBuilder builder = ItemPurchased.builder();
        String departmentSelected = randomDepartment();
        logger.info("randomDepartment: " + departmentSelected);

        //Date calendarA = (new GregorianCalendar(2019,0,1)).getTime();
       // Date calendarB = (new GregorianCalendar(2019,11,31)).getTime();
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

        String item;
        String department;
        switch (departmentSelected) {
            case "book":
                item = faker.book().title();
                department = "book";
                break;
            case "beer":
                item = faker.beer().name();
                department = "beer";
                break;
            default:
                item = faker.food().ingredient();
                department = "food";
        }
        builder.setItem(item);
        builder.setDepartment(department);
        builder.setQuantity(faker.number().numberBetween(1,6));
        builder.setUnitPrice(faker.number().randomDouble(2, 10, 100));
        //"2019-09-22T03:38:18.309Z"
        LocalDateTime localDateTime = LocalDateTime.ofInstant(faker.date().between(before, after).toInstant(), zoneId);

        builder.setPurchasedDate(localDateTime);
        //this.purchasedDate = simpleDateFormat.format();
        return builder.build();
    }

    // chapter 5 KTable
    public static StockTickerData createStockTickerData() {
        return  new StockTickerData(
                symbolList[faker.number().numberBetween(0, symbolList.length -1)],
                faker.number().randomDouble(2, 10, 50));
    }

    public static List<StockTickerData> makeStockTickerData(int iteration) {
        List<StockTickerData> stockTickerDataList = new ArrayList<>();
        for (int i = 0; i < iteration; i++) {
            stockTickerDataList.add(createStockTickerData());
        }

        return stockTickerDataList;
    }

    public static List<StockTransaction> makeStockTx(int iteration, int customerSize, String[] industryList) {
        List<StockTransaction> txList = new ArrayList<>();
        StockTransaction.STransactionBuilder builder = StockTransaction.builder();
        String industry;
        List<String> customerIDList = makeCustomerID(customerSize);
        for (int i = 0; i < iteration; i++) {
            industry = industryList[faker.number().numberBetween(0, industryList.length -1)];
            txList.add(builder.customerId(customerIDList.get(faker.number().numberBetween(0, customerSize - 1)))
                    .industry(industry)
                    .transactionTimestamp(LocalDateTime.now())
                    .sector(faker.company().name())
                    .purchase(faker.number().numberBetween(1,3) == 1)
                    .sharePrice(faker.number().randomDouble(2, 10, 200))
                    .shares(faker.number().numberBetween(100, 1000))
                    .symbol(industry + "_" + faker.number().numberBetween(1,5)).build());
        }
        return txList;
    }

    public static List<String> makeCustomerID(int size) {
        List<String> customerID = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            customerID.add("cid-" + i);
        }
        return customerID;
    }


    public static List<StockTransaction> makeStockTxInTradingWindow(
            int txCount,
            String [] customerIDList,
            String[] industryList,
            LocalDateTime localDateTimeStart,
            int tradingWindowInSeconds) {
        List<StockTransaction> txList = new ArrayList<>();
        StockTransaction.STransactionBuilder builder = StockTransaction.builder();

        String industry;
        String cid;
        LocalDateTime purchaseTime;
        for (int i = 0; i < txCount; i++) {
            // randomly pick simulated data
            industry = industryList[faker.number().numberBetween(0, industryList.length - 1)];
            cid = customerIDList[faker.number().numberBetween(0, customerIDList.length - 1)];
            if (i == 0) {
                purchaseTime = localDateTimeStart;
            } else {
                purchaseTime = localDateTimeStart.plusSeconds(faker.number().numberBetween(0, tradingWindowInSeconds));
            }
            txList.add(builder.customerId(cid)
                    .industry(industry)
                    .transactionTimestamp(purchaseTime)
                    .sector(faker.company().name())
                    .purchase(faker.number().numberBetween(1, 3) == 1)
                    .sharePrice(faker.number().randomDouble(2, 10, 200))
                    .shares(faker.number().numberBetween(100, 1000))
                    .symbol(industry + "_" + faker.number().numberBetween(1, 2)).build());
        }
        return txList;
    }
    /***
     * makeStockTxWithinWindow creates a random transactions executed by
     * customers occurred in a randomly selected trading sessions within
     * the range of windowSizeInMinute.
     *
     * @param iteration number of transactions
     * @param customerCount the number of customer
     * @param windowSizeInSeconds array of window size in seconds
     * @param windowGapsInSeconds gap in seconds before the start of a new trading window
     * @return
     */
    public static List<StockTransaction> makeStockTxWithinWindow(
            int iteration, int customerCount,
            int minTxPerWindow, int maxTxPerWindow,
            int[] windowSizeInSeconds, int[] windowGapsInSeconds,
            String[] industries) {

        List<StockTransaction> stockTransactions = new ArrayList<>();
        String[] customersId = new String[customerCount];

        for (int i = 0; i < customerCount; i++) {
            customersId[i] = "c-" + i;
        }

        //String[] industries = {"food", "tooy", "book"};
        LocalDateTime localDateTime = LocalDateTime.now();
        localDateTime = localDateTime.withSecond(0).withNano(0);
        int gapInSecond;
        int windowSizeInSec;
        int txCount;
        LocalDateTime endTime;
        for(int i = 0; i < iteration; i++) {
            txCount = faker.number().numberBetween(minTxPerWindow, maxTxPerWindow);
            windowSizeInSec = windowSizeInSeconds[faker.number().numberBetween(0, windowSizeInSeconds.length - 1)];
            gapInSecond = windowGapsInSeconds[faker.number().numberBetween(0, windowGapsInSeconds.length - 1)];
            logger.info("tx count - " + txCount +
                            ", window size: " + windowSizeInSec +
                            ", gap: " + gapInSecond);
            List<StockTransaction> newTx = makeStockTxInTradingWindow(
                    txCount, customersId, industries,localDateTime, windowSizeInSec);
            endTime = newTx.get(0).getTransactionTimestamp();

            for (StockTransaction tx: newTx) {
                if (tx.getTransactionTimestamp().isAfter(endTime)) {
                    endTime = tx.getTransactionTimestamp();
                }
            }
            stockTransactions.addAll(newTx);
            //endTime = stockTransactions.get(stockTransactions.size() - 1).getTransactionTimestamp();
            localDateTime =  endTime.plusSeconds(gapInSecond);
        }
        
        return stockTransactions;
    }

    public static List<String> makeFinancialNews(int max) {
        List<String> financialNews = new ArrayList<>();

        for (int i = 0; i < max; i++) {
            financialNews.add(faker.company().bs());
        }

        return financialNews;
    }

    public static List<BeerDistribution> makeBeerDistribution(int max) {
        List<BeerDistribution> beerDistributions = new ArrayList<>();
        BeerDistribution.BeerDistributionBuilder builder = BeerDistribution.builder();
        BeerDistribution distribution;

        for (int i = 0; i < max; i++) {
            builder
                    .beerType(BEER_TYPE[faker.number().numberBetween(0, BEER_TYPE.length)])
                    .currency(getCurrency(faker.number().numberBetween(0, 2)))
                    .numberOfCases(faker.number().numberBetween(20, 100))
                    .totalSales(faker.number().numberBetween(500, 3000));

            beerDistributions.add(builder.build());
        }

        return beerDistributions;
    }

    public static List<ClickEvent> makeClickEvents(int max, List<String> industrylList) {
        List<ClickEvent> clickEvents = new ArrayList<>();
        List<String> symbolList = new ArrayList<>();
        for (String industry: industrylList) {
            symbolList.add(symbolSupplier.apply(industry));
        }
        Instant passed5min = Instant.now().minus(5, ChronoUnit.MINUTES);
        for (int i = 0; i< max; i++) {
            String symbol = symbolList.get(faker.number().numberBetween(0, symbolList.size() - 1));
            ClickEvent event = new ClickEvent(
                    symbol,
                            faker.date().between(Date.from(passed5min),
                                    Date.from(passed5min.plus(3, ChronoUnit.MINUTES))).toInstant(),
                    "http://" + symbol + "-" + faker.company().url());
            clickEvents.add(event);
        }
        return clickEvents;
    }

    private static Currency getCurrency(int i) {
        if (i == 0) {
            return Currency.POUNDS;
        } else if (i ==1 ) {
            return Currency.DOLLARS;
        } else {
            return Currency.EUROS;
        }
    }
}
