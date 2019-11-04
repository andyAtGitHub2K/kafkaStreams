package ahbun.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ShareVolume {
    private String industry;
    private String symbol;
    private int volume;
    private static Logger logger = LoggerFactory.getLogger(ShareVolume.class);
    public static ShareVolumeBuilder builder() { return new ShareVolumeBuilder(); }
    public static ShareVolumeBuilder builder(final StockTransaction transaction) {
        ShareVolumeBuilder builder = builder();
        builder.industry(transaction.getIndustry())
                .symbol(transaction.getSymbol())
                .volume(transaction.getShares());

        return builder;
    }
    public static ShareVolumeBuilder builder(final ShareVolume shareVolume) {
        ShareVolumeBuilder builder = builder();
        builder.industry(shareVolume.industry)
                .volume(shareVolume.volume)
                .symbol(shareVolume.symbol);
        return builder;
    }

    public static ShareVolume sum(ShareVolume sv1, ShareVolume sv2) {
        logger.info("Sum sv1: " + sv1.volume + ", sv2: " + sv2.volume +
                " inducstry: sv1: " + sv1.industry + ", sv2: " + sv2.industry);
        ShareVolumeBuilder builder = builder(sv1);
        builder.volume += sv2.volume;
        return builder.build();
    }

    public ShareVolume(String industry, String symbol, int volume) {
        this.industry = industry;
        this.symbol = symbol;
        this.volume = volume;
    }

    private ShareVolume(final ShareVolumeBuilder builder) {
        this.industry = builder.industry;
        this.symbol = builder.symbol;
        this.volume = builder.volume;
    }

    private ShareVolume(){}

    public String getIndustry() {
        logger.debug("Get Industry called");
        return industry;
    }

    public String getSymbol() {
        return symbol;
    }

    public int getVolume() {
        return volume;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShareVolume that = (ShareVolume) o;
        return volume == that.volume &&
                industry.equals(that.industry) &&
                symbol.equals(that.symbol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(industry, symbol, volume);
    }

    @Override
    public String toString() {
        return "{" +
                "\"industry\":\"" + industry + '"' +
                ",\"symbol\":\"" + symbol + '"' +
                ",\"volume\":" + volume +
                '}';
    }

    public static final class ShareVolumeBuilder {
        private String industry;
        private String symbol;
        private int volume;
        private static Logger logger = LoggerFactory.getLogger(ShareVolumeBuilder.class);
        public ShareVolume build() {
            return new ShareVolume(this);
        }

        private ShareVolumeBuilder(){}

        public ShareVolumeBuilder industry(String industry) {
            this.industry = industry;
            return this;
        }

        public ShareVolumeBuilder symbol(String symbol) {
            this.symbol = symbol;
            return this;
        }

        public ShareVolumeBuilder volume(int volume) {
            this.volume = volume;
            return this;
        }
    }
}
