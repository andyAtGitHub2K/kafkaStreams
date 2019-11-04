package ahbun.chapter2.partitioner;
import ahbun.chapter2.entity.PurchaseKey;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;

/***
 * PurchaseKeyPartitioner implements a custom partition on PurchaseKey
 */
public class PurchaseKeyPartitioner extends DefaultPartitioner {
    @Override
    /**
     * override the partition method to compute partition value
     * from client id of the PurchaseKey.
     */
    public int partition(
            String topic,
            Object key, byte[] keyBytes,
            Object value, byte[] valueBytes,
            Cluster cluster
    ) {
        String newKey = null;
        if (key != null) {
            PurchaseKey pk = (PurchaseKey) key;
            newKey = pk.getClientId();
            keyBytes = newKey.getBytes();
        }

        return super.partition(topic, newKey, keyBytes,
                value, valueBytes, cluster);
    }
}
