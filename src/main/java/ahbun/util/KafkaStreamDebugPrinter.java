package ahbun.util;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/***
 * KafkaDebugPrinter
 */
public class KafkaStreamDebugPrinter implements ProcessorSupplier {
    private String name;

    public KafkaStreamDebugPrinter(String name) {
        this.name = name;
    }

    @Override
    public Processor get() {
        return new PrintingProcessor(this);
    }

    private class PrintingProcessor extends AbstractProcessor {
        private String name;

        private PrintingProcessor(KafkaStreamDebugPrinter printer) {
            this.name = printer.name;
        }

        @Override
        public void process(Object key, Object value) {
            System.out.printf("[%s]: key: %s, value: %s\n",
                    name, key, value);
            this.context().forward(key, value);
        }
    }
}
