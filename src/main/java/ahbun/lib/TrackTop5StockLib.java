package ahbun.lib;

import ahbun.model.ShareVolume;
import ahbun.model.StockTransaction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Properties;

public class TrackTop5StockLib {
   private static NumberFormat numberFormat = NumberFormat.getInstance();
   private static Properties appProperties;

   static {
      try {
         appProperties = new Properties();
         appProperties.load(Objects
                        .requireNonNull(
                        ClassLoader
                                .getSystemResourceAsStream("chapter5/kafkaTopVolumeAppConfig.properties")));
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   public static ValueMapper<StockTransaction, ShareVolume> txToShareVolume =
            tx -> ShareVolume.builder(tx).build();
   // re-map key to symbol
   public static KeyValueMapper<String, ShareVolume, String> reMapSymbolAsKey
           = (key, value) -> value.getSymbol();

   public static Comparator<ShareVolume> comparePQByVolume = (sv1, sv2) -> sv2.getVolume() - sv1.getVolume();

   // value mapper collects the industry name, and respective trading symbol and its
   // respective volume from the queue and generates the aggregated information as string.
   public static ValueMapper<FixedSizePriorityQueue, String> mapPQToTop5Summary = fpq -> {
      StringBuilder stringBuilder = new StringBuilder();
      Iterator<ShareVolume> iterator = fpq.iterator();
      int counter = 1;
      boolean isIndustrySet = false;
      while (iterator.hasNext()) {
         ShareVolume stockVolume = iterator.next();
         if (stockVolume != null) {
            if (!isIndustrySet) {
               stringBuilder.append("[" + stockVolume.getIndustry() + "]: ");
               isIndustrySet = true;
            }
            stringBuilder.append(counter++).append(")").append(stockVolume.getSymbol())
                    .append(":").append(numberFormat.format(stockVolume.getVolume())).append(" ");
         }
      }
      return stringBuilder.toString();
   };

   public static KeyValueMapper<String, ShareVolume, KeyValue<String, ShareVolume>>
           mapIndustryAsKey = (k,v) -> KeyValue.pair(v.getIndustry(), v);

   public static Initializer<FixedSizePriorityQueue> fixedSizePriorityQueueInitializer =
           ()->  new FixedSizePriorityQueue<>(TrackTop5StockLib.comparePQByVolume,
                   Integer.parseInt(appProperties.getProperty("top.n")));

}
