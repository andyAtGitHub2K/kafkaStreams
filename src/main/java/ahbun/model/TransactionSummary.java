package ahbun.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Objects;

/***
 * TransactionSummary represents a summary from the
 * StockTransaction.
 */
public class TransactionSummary {
    private String customerId;
    private String symbol;
    private String industry;
    private long summaryCount;

  //  LocalDateTime time;
    private static Logger logger = LoggerFactory.getLogger(TransactionSummary.class);
    public static TransactionSummaryBuilder builder() {
        return new TransactionSummaryBuilder();
    }

    public static TransactionSummaryBuilder builder(StockTransaction tx) {
        TransactionSummaryBuilder builder = builder();
        logger.info("Preparing " + tx.getCustomerId() + "," + tx.getSymbol() +
                tx.getTransactionTimestamp() );
        builder
                .customerId(tx.getCustomerId())
                .symbol(tx.getSymbol())
                .industry(tx.getIndustry());
               // .time(tx.getTransactionTimestamp());
        return builder;
    }

    private TransactionSummary(final TransactionSummaryBuilder builder) {
        customerId = builder.getCustomerId();
        symbol = builder.getSymbol();
        industry = builder.getIndustry();
   //     time = builder.getTime();
    }

    private TransactionSummary() {}

    public String getCustomerId() {
        return customerId;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getIndustry() {
        return industry;
    }

//    public LocalDateTime getTime() {
//        return time;
//    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionSummary summary = (TransactionSummary) o;
        return customerId.equals(summary.customerId) &&
                symbol.equals(summary.symbol) &&
                industry.equals(summary.industry) &&
                summaryCount == summary.summaryCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(customerId, symbol, industry, summaryCount);
    }

    @Override
    public String toString() {
        return "TransactionSummary{" +
                "customerId='" + customerId + '\'' +
                ", symbol='" + symbol + '\'' +
                ", industry='" + industry + '\'' +
                ", count=" + summaryCount +
            //    ", time=" + time +
                '}';
    }

    public long getSummaryCount() {
        return summaryCount;
    }

    public TransactionSummary summaryCount(long summaryCount) {
        this.summaryCount = summaryCount;
        return this;
    }


    public static class TransactionSummaryBuilder {
        String customerId;
        String symbol;
        String industry;
     //   LocalDateTime time;

        // generate composite key for TransactionSummary
        public static String getKeyFrom(StockTransaction tx) {
            return tx.getCustomerId() + "-" + tx.getSymbol();
        }

        public TransactionSummary build() {
            return new TransactionSummary(this);
        }

        private  TransactionSummaryBuilder() {}

        public String getCustomerId() {
            return customerId;
        }

        public TransactionSummaryBuilder customerId(String customerId) {
            this.customerId = customerId;
            return this;
        }

        public String getSymbol() {
            return symbol;
        }

        public TransactionSummaryBuilder symbol(String symbol) {
            this.symbol = symbol;
            return this;
        }


        public String getIndustry() {
            return industry;
        }

        public TransactionSummaryBuilder industry(String industry) {
            this.industry = industry;
            return this;
        }

//        public LocalDateTime getTime() {
//            return time;
//        }
//
//        public TransactionSummaryBuilder time(LocalDateTime time) {
//            this.time = time;
//            return this;
//        }
    }
}
