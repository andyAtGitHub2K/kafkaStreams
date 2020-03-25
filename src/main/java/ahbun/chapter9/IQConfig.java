package ahbun.chapter9;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;

public class IQConfig {
    private String [] customerIDList;
    private String[] industryList;
    private String[] sectorList;
    private String[] symbolList;
    private LocalDateTime localDateTimeStart;
    private int tradingWindowInSeconds;

    public IQConfig(String [] customerIDList,
                    String[] industryList,
                    String[] sectorList,
                    String[] symbolList,
                    LocalDateTime localDateTimeStart,
                    int tradingWindowInSeconds) {
        this.customerIDList = customerIDList;
        this.industryList = industryList;
        this.sectorList = sectorList;
        this.symbolList = symbolList;
        this.localDateTimeStart = localDateTimeStart;
        this.tradingWindowInSeconds = tradingWindowInSeconds;
    }

    public String[] getCustomerIDList() {
        return customerIDList;
    }

    public IQConfig setCustomerIDList(String[] customerIDList) {
        this.customerIDList = customerIDList;
        return this;
    }

    public String[] getIndustryList() {
        return industryList;
    }

    public IQConfig setIndustryList(String[] industryList) {
        this.industryList = industryList;
        return this;
    }

    public String[] getSectorList() {
        return sectorList;
    }

    public IQConfig setSectorList(String[] sectorList) {
        this.sectorList = sectorList;
        return this;
    }

    public String[] getSymbolList() {
        return symbolList;
    }

    public IQConfig setSymbolList(String[] symbolList) {
        this.symbolList = symbolList;
        return this;
    }

    public LocalDateTime getLocalDateTimeStart() {
        return localDateTimeStart;
    }

    public IQConfig setLocalDateTimeStart(LocalDateTime localDateTimeStart) {
        this.localDateTimeStart = localDateTimeStart;
        return this;
    }

    public int getTradingWindowInSeconds() {
        return tradingWindowInSeconds;
    }

    public IQConfig setTradingWindowInSeconds(int tradingWindowInSeconds) {
        this.tradingWindowInSeconds = tradingWindowInSeconds;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IQConfig iqConfig = (IQConfig) o;
        return tradingWindowInSeconds == iqConfig.tradingWindowInSeconds &&
                Arrays.equals(customerIDList, iqConfig.customerIDList) &&
                Arrays.equals(industryList, iqConfig.industryList) &&
                Arrays.equals(sectorList, iqConfig.sectorList) &&
                Arrays.equals(symbolList, iqConfig.symbolList) &&
                localDateTimeStart.equals(iqConfig.localDateTimeStart);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(localDateTimeStart, tradingWindowInSeconds);
        result = 31 * result + Arrays.hashCode(customerIDList);
        result = 31 * result + Arrays.hashCode(industryList);
        result = 31 * result + Arrays.hashCode(sectorList);
        result = 31 * result + Arrays.hashCode(symbolList);
        return result;
    }

    @Override
    public String toString() {
        return "IQConfig{" +
                "customerIDList=" + Arrays.toString(customerIDList) +
                ", industryList=" + Arrays.toString(industryList) +
                ", sectorList=" + Arrays.toString(sectorList) +
                ", symbolList=" + Arrays.toString(symbolList) +
                ", localDateTimeStart=" + localDateTimeStart +
                ", tradingWindowInSeconds=" + tradingWindowInSeconds +
                '}';
    }
}
