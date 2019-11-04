package ahbun.model;

import java.time.LocalDateTime;
import java.util.Objects;

/***
 * symbol, sector, industry, shares
 *  *                        share prices, customerId,
 *  *                        transactionTimeStamp, purchase or sell
 */
public class StockTransaction {
    String symbol;
    String sector;
    String industry;
    int shares;
    double sharePrice;
    String customerId;
    LocalDateTime transactionTimestamp;
    boolean purchase;

    public static STransactionBuilder builder() { return new STransactionBuilder(); }
    public static STransactionBuilder builder(final StockTransaction st) {
        STransactionBuilder builder = builder();
        builder.customerId = st.customerId;
        builder.industry = st.industry;
        builder.purchase = st.purchase;
        builder.sector = st.sector;
        builder.sharePrice = st.sharePrice;
        builder.shares = st.shares;
        builder.transactionTimestamp = st.transactionTimestamp;
        builder.symbol = st.symbol;
        return builder;
    }

    private StockTransaction(final STransactionBuilder builder){
        this.customerId = builder.customerId;
        this.industry = builder.industry;
        this.purchase = builder.purchase;
        this.sector = builder.sector;
        this.sharePrice = builder.sharePrice;
        this.symbol = builder.symbol;
        this.shares = builder.shares;
        this.transactionTimestamp = builder.transactionTimestamp;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getSector() {
        return sector;
    }

    public String getIndustry() {
        return industry;
    }

    public int getShares() {
        return shares;
    }

    public double getSharePrice() {
        return sharePrice;
    }

    public String getCustomerId() {
        return customerId;
    }

    public LocalDateTime getTransactionTimestamp() {
        return transactionTimestamp;
    }

    public boolean isPurchase() {
        return purchase;
    }

    @Override
    public String toString() {
        return "StockTransaction{" +
                "symbol='" + symbol + '\'' +
                ", sector='" + sector + '\'' +
                ", industry='" + industry + '\'' +
                ", shares=" + shares +
                ", sharePrice=" + sharePrice +
                ", customerId='" + customerId + '\'' +
                ", transactionTimestamp=" + transactionTimestamp +
                ", purchase=" + purchase +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StockTransaction that = (StockTransaction) o;
        return shares == that.shares &&
                Double.compare(that.sharePrice, sharePrice) == 0 &&
                purchase == that.purchase &&
                symbol.equals(that.symbol) &&
                sector.equals(that.sector) &&
                industry.equals(that.industry) &&
                customerId.equals(that.customerId) &&
                transactionTimestamp.equals(that.transactionTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, sector, industry, shares, sharePrice, customerId, transactionTimestamp, purchase);
    }

    public final static class STransactionBuilder {
        String symbol;
        String sector;
        String industry;
        int shares;
        double sharePrice;
        String customerId;
        LocalDateTime transactionTimestamp;
        boolean purchase;

        public StockTransaction build() { return new StockTransaction(this);}

        private STransactionBuilder(){}


        public boolean isPurchase() {
            return purchase;
        }

        public STransactionBuilder symbol(String symbol) {
            this.symbol = symbol;
            return this;
        }

        public STransactionBuilder sector(String sector) {
            this.sector = sector;
            return this;
        }

        public STransactionBuilder industry(String industry) {
            this.industry = industry;
            return this;
        }

        public STransactionBuilder shares(int shares) {
            this.shares = shares;
            return this;
        }

        public STransactionBuilder sharePrice(double sharePrice) {
            this.sharePrice = sharePrice;
            return this;
        }

        public STransactionBuilder customerId(String customerId) {
            this.customerId = customerId;
            return this;
        }

        public STransactionBuilder transactionTimestamp(LocalDateTime transactionTimestamp) {
            this.transactionTimestamp = transactionTimestamp;
            return this;
        }

        public STransactionBuilder purchase(boolean purchase) {
            this.purchase = purchase;
            return this;
        }
    }

    private StockTransaction(){}
}
