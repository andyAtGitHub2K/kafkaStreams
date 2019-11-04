package ahbun.chapter2.ahbun.chapter.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.*;

/***
 * ThreadedConsumer consume partitioned topic using a pool of executor
 * to poll messages in regular interval.
 */
public class ThreadedConsumer {
    private boolean done = false;
    private final int partitions;
    private final Duration duration;
    private final String topic;
    private final ExecutorService executorService;
    private final Properties consumerProperties;

    public ThreadedConsumer(String topic, int partitions, int pollTimeout) throws IOException {
        this.partitions = partitions;
        this.topic = topic;
        duration = Duration.ofMillis(pollTimeout);

        // create a pool of executors
        executorService = Executors.newFixedThreadPool(partitions);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("chapter2/sample_kafka_consumer.properties");
        consumerProperties = new Properties();
        consumerProperties.load(inputStream);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ThreadedConsumer consumer = new ThreadedConsumer("chapter2", 4, 5000);
        consumer.executeTask();
        Thread.sleep(30000);
        consumer.terminateTasks();
    }


    public void executeTask() {
        for (int i = 0; i < partitions; i++) {
            Runnable task = consumerTask();
            executorService.submit(task);
        }
    }


    // implement runnable as a task to handle partitioned message
    // 1) create a Consumer
    // 2) subscribe to topic
    // 3) poll records at regular interval
    // 4) handle message
    private Runnable consumerTask() {
        return () -> {

            Consumer<String, String> consumer = null;
            try {
                // 1) create a Consumer
                consumer = new KafkaConsumer<>(this.consumerProperties);
                // 2) subscribe to topic
                consumer.subscribe(Collections.singletonList(this.topic));
                while (!this.done) {
                    // 3) poll records at regular interval
                    ConsumerRecords<String, String> records = consumer.poll(this.duration);

                    for(ConsumerRecord<String, String> record: records) {
                        String message = String.format("Consumer: key=%s, value=%s, partitio=%d, offset=%d",
                                record.key(), record.value(), record.partition(), record.offset());
                        System.out.println(message);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        };
    }

    private void terminateTasks() throws InterruptedException {
        done = true;
        executorService.awaitTermination(10000, TimeUnit.MILLISECONDS);
        executorService.shutdownNow();
    }
}
