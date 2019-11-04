package ahbun.chapter2.producer;

import ahbun.chapter2.entity.PurchaseKey;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducer {
    public static void main(String [] argv) throws IOException, InterruptedException, ExecutionException {
        // load properties from resources
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream("chapter2/sample_kafka_producer.properties");
        Properties properties = new Properties();
        properties.load(is);

        // create a purchase key
        PurchaseKey pk = new PurchaseKey("a", new Date());
        String[] ids = new String[]{"a", "b", "c", "d", "e", "f"};

        List<PurchaseKey> pkList = new ArrayList<>();
        for(String id : ids) {
            pkList.add(new PurchaseKey(id, new Date()));
            Thread.sleep(300);
        }


        // instantiate a producer <key, value> and send a producer record
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            // create a record
            ProducerRecord<String, String> record;

            Callback callback = ((metadata, exception) -> {
               if (exception != null) {
                   exception.printStackTrace();
               } else {

                   System.out.printf("send success: %s\n", metadata.toString());
               }
            });

            List<Future<RecordMetadata>> listOfFuture =  new ArrayList<>();
            for (PurchaseKey purchaseKey : pkList) {
                record = new ProducerRecord<String, String >("chapter2", purchaseKey.getClientId(), "value");
                listOfFuture.add(producer.send(record, callback));
                //Future<RecordMetadata> future = producer.send(record, callback);
            }

            System.out.println("done sending message");

            for (Future<RecordMetadata> future: listOfFuture) {
                if (future.isDone()) {
                    RecordMetadata recordMetadata = future.get();
                    System.out.println("future is done");
                    System.out.printf(recordMetadata.toString());
                }
            }
        }
    }
}
