package ahbun.model;

import java.util.Objects;

/***
 * BeerDistribution represents the sales of beer in different region of the world
 * reflected from the type of currency in the beer distribution.
 *
 * Currency, totalSales, numberOfCases, beerType
 */
public class BeerDistribution {
    private Currency currency;
    private double totalSales;
    private int numberOfCases;
    private String beerType;

    public static BeerDistributionBuilder builder() { return new BeerDistributionBuilder(); }
    public static BeerDistributionBuilder builder(BeerDistribution beerDistribution) {
        BeerDistributionBuilder beerDistributionBuilder = builder();
        beerDistributionBuilder
                .numberOfCases(beerDistribution.numberOfCases)
                .totalSales(beerDistribution.totalSales)
                .currency(beerDistribution.currency)
                .beerType(beerDistribution.beerType);
        return  beerDistributionBuilder;
    }
    private BeerDistribution(BeerDistributionBuilder builder) {
        this.currency = builder.getCurrency();
        this.beerType = builder.getBeerType();
        this.numberOfCases = builder.getNumberOfCases();
        this.totalSales = builder.getTotalSales();
    }

    private BeerDistribution(){}

    public Currency getCurrency() {
        return currency;
    }

    public double getTotalSales() {
        return totalSales;
    }

    public int getNumberOfCases() {
        return numberOfCases;
    }

    public String getBeerType() {
        return beerType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BeerDistribution that = (BeerDistribution) o;
        return Double.compare(that.totalSales, totalSales) == 0 &&
                numberOfCases == that.numberOfCases &&
                currency == that.currency &&
                beerType.equals(that.beerType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currency, totalSales, numberOfCases, beerType);
    }

    @Override
    public String toString() {
        return "BeerDistribution{" +
                "currency=" + currency +
                ", totalSales=" + totalSales +
                ", numberOfCases=" + numberOfCases +
                ", beerType='" + beerType + '\'' +
                '}';
    }

    public static class BeerDistributionBuilder {
        private Currency currency;
        private double totalSales;
        private int numberOfCases;
        private String beerType;

        public BeerDistribution build() { return new BeerDistribution(this); }

        public Currency getCurrency() {
            return currency;
        }

        public BeerDistributionBuilder currency(Currency currency) {
            this.currency = currency;
            return this;
        }

        public double getTotalSales() {
            return totalSales;
        }

        public BeerDistributionBuilder totalSales(double totalSales) {
            this.totalSales = totalSales;
            return this;
        }

        public int getNumberOfCases() {
            return numberOfCases;
        }

        public BeerDistributionBuilder numberOfCases(int numberOfCases) {
            this.numberOfCases = numberOfCases;
            return this;
        }

        public String getBeerType() {
            return beerType;
        }

        public BeerDistributionBuilder beerType(String beerType) {
            this.beerType = beerType;
            return this;
        }
    }
}
