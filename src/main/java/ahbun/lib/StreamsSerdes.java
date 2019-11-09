package ahbun.lib;

import ahbun.model.*;
import ahbun.util.Tuple;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;


import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class StreamsSerdes {
    public static class RewardSerde extends WrapperSerdes<Reward> {
        public RewardSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(Reward.class));
        }
    }

    public static class PurchasePatternSerde extends WrapperSerdes<PurchasePattern> {
        public PurchasePatternSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(PurchasePattern.class));
        }
    }

    public static class StockTickerSerde extends  WrapperSerdes<StockTickerData> {
        public StockTickerSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(StockTickerData.class));
        }
    }

    public static Serde<StockTransaction> StockTransactionSerde() {
        return new StockTransactionSerde();
    }

    public static class StockTransactionSerde extends WrapperSerdes<StockTransaction> {
        public StockTransactionSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(StockTransaction.class));
        }
    }

    public static Serde<ShareVolume> ShareVolumeSerde() {
        return new ShareVolumeSerde();
    }

    public static class ShareVolumeSerde extends WrapperSerdes<ShareVolume> {
        public ShareVolumeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(ShareVolume.class));
        }
    }

    public static Serde<FixedSizePriorityQueue> FSPQS() {
        return new FixedSizePQSerde();
    }

    public static final class FixedSizePQSerde extends WrapperSerdes<FixedSizePriorityQueue> {
        public FixedSizePQSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(FixedSizePriorityQueue.class));
        }
    }

    /**
     * PurchaseSerde is a type of Serde
     */
    public static class PurchaseSerde extends WrapperSerdes<Purchase> {
        public PurchaseSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(Purchase.class));
        }
    }

    public static class RewardAccumatorSerde extends WrapperSerdes<RewardAccumulator> {
        public RewardAccumatorSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(RewardAccumulator.class));
        }
    }

    public static Serde<TransactionSummary> TxSummarySerde() {
        return new TxSummarySerde();
    }

    public static class TxSummarySerde extends WrapperSerdes<TransactionSummary> {
        public TxSummarySerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(TransactionSummary.class));
        }
    }

    public static Serde<StockPerformance> StockPerfSerde() {
        return new StockPerfSerde();
    }

    public static class StockPerfSerde extends WrapperSerdes<StockPerformance> {

        public StockPerfSerde() {
           super(new JsonSerializer<>(), new JsonDeserializer<>(StockPerformance.class));
        }
    }

    public static Serde<ClickEvent> ClickEventSerde() {
        return new ClickEventSerde();
    }

    public static class ClickEventSerde extends WrapperSerdes<ClickEvent> {
        public ClickEventSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(ClickEvent.class));
        }
    }

    public static Serde<Tuple<List<ClickEvent>, List<StockTransaction>>> CTSerceTupleSerde() {
        return  new CTSerceTuple();
    }

    public static class CTSerceTuple  extends WrapperSerdes<Tuple<List<ClickEvent>, List<StockTransaction>>> {
        private static final Type type = new TypeToken<Tuple<List<ClickEvent>, List<StockTransaction>>>(){}.getType();
        public CTSerceTuple() {
            super(new JsonSerializer<>(),  new JsonDeserializer<>(type));
        }
    }

    /***
     * WrapperSerdes provide methods to retrieve Serdes for  type T
     * @param <T>
     */
    private static class WrapperSerdes<T> implements Serde<T> {
        private JsonSerializer<T> serializer;
        private JsonDeserializer<T> deserializer;

        WrapperSerdes(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
            this.serializer= serializer;
            this.deserializer = deserializer;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<T> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<T> deserializer() {
            return deserializer;
        }
    }
}
