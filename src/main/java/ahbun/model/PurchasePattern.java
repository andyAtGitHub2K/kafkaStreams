package ahbun.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;

public class PurchasePattern {
    private String zipCode;
    private String item;
    private LocalDateTime date;
    private double amount;

    /**
     * static method to instantiate builder object
     * @return
     */
    public static PatternBuilder builder() { return new PatternBuilder(); }

    /**
     * initialize the builder instance for a given PurchasePattern
     * @param pp
     * @return
     */
    public static PatternBuilder init(final Purchase pp) {
        PatternBuilder builder = builder();
        builder
                .zipCode(pp.getZipCode())
                .item(pp.getItemPurchased())
                .date(pp.getPurchaseDate())
                .amount(pp.getPrice() * pp.getQuantity());
        return builder;
    }

    public String getZipCode() {
        return zipCode;
    }

    public String getItem() {
        return item;
    }

    public LocalDateTime getDate() {
        return date;
    }

    public double getAmount() {
        return amount;
    }


    /**
     * Delegate the PurchasePattern instantiation to the builder
     * @param builder
     */
    private PurchasePattern(PatternBuilder builder) {
        this.amount = builder.amount;
        this.date = builder.date;
        this.item = builder.item;
        this.zipCode = builder.zipCode;
    }
    private static Date convertStrToDate(final String dateInStr) throws ParseException {
        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        return d.parse(dateInStr);
    }

    @Override
    public String toString() {
        return "PurchasePattern{" +
                "zipCode='" + zipCode + '\'' +
                ", item='" + item + '\'' +
                ", date=" + date +
                ", amount=" + amount +
                '}';
    }

    public static class PatternBuilder {
        private String zipCode;
        private String item;
        private LocalDateTime date;
        private double amount;

        public PatternBuilder builder() { return new PatternBuilder(); }

        /**
         * Instantiate Purchase Pattern object using the builder
         * @return
         */
        public PurchasePattern build() {
            return new PurchasePattern(this);
        }

        private PatternBuilder(){}

        public PatternBuilder zipCode(String zipCode) {
            this.zipCode = zipCode;
            return this;
        }

        public PatternBuilder item(String item) {
            this.item = item;
            return this;
        }

        public PatternBuilder date(LocalDateTime date) {
            this.date = date;
            return this;
        }

        public PatternBuilder amount(double amount) {
            this.amount = amount;
            return this;
        }
    }
}
