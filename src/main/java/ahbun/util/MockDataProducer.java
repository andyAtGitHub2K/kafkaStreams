package ahbun.util;

import ahbun.model.Purchase;
import ahbun.model.StockTickerData;
import ahbun.model.StockTransaction;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.producer.Callback;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ahbun.util.DataGenerator.makeFinancialNews;
import static ahbun.util.DataGenerator.makeStockTx;

/***
 * MockDataProducer generate kafka messages orginated from a data generator
 */
public class MockDataProducer {
    private static Logger logger = LoggerFactory.getLogger(MockDataProducer.class);
    private static String topic;
    private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private static Producer<String, String> producer;
    private static ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static Callback callback;
    private static boolean runForever = true;


    /***
     * producePurchaseData initializes a kafka producer to send message.
     * Process: create a Runnable that perform the followings:
     *          1. initialize the producer
     *          2. set up an iteration loop to generate purchase message for the producer
     *             to send to purchase topic
     *
     *          use the executor service to submit the runnable job.
     * @param recipe
     */
    public static void producePurchaseData(PurchaseSimiluationRecipe recipe,
                                           String topic,
                                           long messageIntervalMills) {
        // create a Runnable
        Runnable generatePurchaseMessage = () -> {
            try {

                init();
                logger.info("init completed");
                List<Purchase> purchaseList = DataGenerator.createPurchase(recipe);
                List<String> jsonList = convertToJson(purchaseList);
                for (String value : jsonList) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, value);
                    producer.send(record, callback);
                    logger.info("producer send: " + value);
                    Thread.sleep(messageIntervalMills);
                }
                logger.info("batch send is completed.");
            } catch (IOException | ParseException | InterruptedException ex) {
                logger.info(ex.getMessage());
                return;
            }

            logger.info("purchase data generation is completed.");
        };

        executorService.submit(generatePurchaseMessage);
    }


    public static void produceStockData(int iterations,
                                        String streamtopic,
                                        String tableTopic,
                                        long messageIntervalMills) {
        Runnable generateStockData = () -> {
          try {
              init();
              List<StockTickerData> stockTickerDataList = DataGenerator.makeStockTickerData(4);
              List<String> jsonList = convertToJson(stockTickerDataList);
              //logger.info("list size = " + jsonList.size());
              //List<StockTickerData> stockTickerDataList = new ArrayList<>();
              StockTickerData stockTickerData = new StockTickerData("abc", 10.0);
              stockTickerDataList.add(stockTickerData);
              stockTickerData= new StockTickerData("HIJ", 20.0);
              stockTickerDataList.add(stockTickerData);

              stockTickerData= new StockTickerData("xyZ", 30.0);
              stockTickerDataList.add(stockTickerData);
              for (int i = 0; i < iterations; i++) {
                  for (StockTickerData std : stockTickerDataList) {
                      String v = convertToJson(std);//new StockTickerData(std.getSymbol(), std.getPrice()));
                      ProducerRecord<String, String> record = new ProducerRecord<>(streamtopic, std.getSymbol(), v);
                      producer.send(record, callback);
                      record = new ProducerRecord<>(tableTopic, std.getSymbol(), v);
                      producer.send(record, callback);
                      std.updatePrice();
                  }
                  Thread.sleep(messageIntervalMills);
                  logger.info("iteration: " + i);
              }
              logger.info("batch send is completed.");

          }catch (Exception ex) {
              Thread.currentThread().interrupt();
              logger.info(ex.getMessage());
              return;
          }
        };

        executorService.submit(generateStockData);
    }

    public static void produceStockTransaction(int iterations,
                                               String sourceTopic,
                                               int batchSize,
                                               long batchIntervalMills,
                                               String financialNewsTopic
                                               ) throws IOException {
        String[] industryList = {"food", "tooy", "book", "sales", "school", "media"};
        if (financialNewsTopic != null) {
            makeFinancialNews(industryList.length);
            produceFinancialNews(financialNewsTopic, industryList);
        }

        Runnable runnable = () -> {
            try {
                init();
                List<StockTransaction> txList;
                for (int i = 0; i < iterations; i++) {
                    txList = DataGenerator.makeStockTx(batchSize, industryList);
                    for (StockTransaction tx : txList) {
                        String json = convertToJson(tx);
                        ProducerRecord<String, String> record =
                                new ProducerRecord<>(sourceTopic, tx.getSymbol(), json);
                        producer.send(record, callback);
                    }
                    logger.info("done batch: " + i);
                    Thread.sleep(batchIntervalMills);
                }
            } catch (IOException | InterruptedException ex) {
                logger.error(ex.getMessage());
            }
        };

        executorService.submit(runnable);
    }

    public static void produceTxWithinWindows(String sourceTopic, int iteration, int customerCount,
                                              int minTxPerWindow, int maxTxPerWindow,
                                              int[] windowSizeInSeconds, int[] windowGapsInSeconds,
                                              String financialNewsTopic,
                                              String[] industryList) throws IOException {

        if (financialNewsTopic != null) {
            produceFinancialNews(financialNewsTopic, industryList);
        }

        Runnable run = () -> {
            try {
                init();
                List<StockTransaction> stockTransactionList = DataGenerator.makeStockTxWithinWindow(
                        iteration, customerCount, minTxPerWindow, maxTxPerWindow,
                        windowSizeInSeconds, windowGapsInSeconds, industryList);
                int count = 0;
                for(StockTransaction tx: stockTransactionList) {
                    String json = convertToJson(tx);
                    ProducerRecord<String, String> record = new ProducerRecord<>(sourceTopic, json);
                    producer.send(record, callback);

                    if (++count % 5 == 0) {
                        Thread.sleep(250);
                    }
                }
            } catch (IOException | InterruptedException ex) {
                System.out.println(ex.getMessage());
            }
        };
        executorService.submit(run);
    }

    public static void produceFinancialNews(String sourceTopic,
                                            String[] industries) throws IOException {
        init();
        List<String> newsList = makeFinancialNews(industries.length);
        int count = 0;
        for (String industry : industries) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(sourceTopic, industry, industry + ": " +newsList.get(count++));
            logger.info("produceFinancialNews: " + industry + " - " + newsList.get(count-1));
            producer.send(record, callback);
        }
    }

    /***
     * initialize Kafka producer and callback
     * @throws IOException
     */
    private static void init() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream("chapter5/kafka_producer_stock.properties");
        Properties properties = new Properties();
        properties.load(is);
        producer = new KafkaProducer<String, String>(properties);

        callback = (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            }
        };
    }

    public static void shutdown() {
        logger.info("shutting down");
        runForever = false;

        if (executorService != null) {
            executorService.shutdown();
            executorService = null;
        }

        if (producer != null) {
            producer.flush();
            producer.close();
            producer = null;
        }

    }

    private static <T> List<String> convertToJson(List<T> generatedData) {
        List<String> jsonString = new ArrayList<>();

        generatedData.forEach(s -> {
            jsonString.add(convertToJson(s));
        });

        return jsonString;
    }

    private static <T> String convertToJson(T object) {
        return gson.toJson(object);
    }

}
