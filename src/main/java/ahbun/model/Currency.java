package ahbun.model;

/***
 * Type of currency
 */
public enum Currency {
    EUROS(1.09),
    POUNDS(1.2929),
    DOLLARS(1.0);

    Currency(double conversionRate) { this.conversionRate = conversionRate; }

    private double conversionRate;

    public double toUSD(double value) { return value / conversionRate; }
}
