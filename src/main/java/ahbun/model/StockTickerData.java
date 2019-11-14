package ahbun.model;

import java.util.Objects;

public class StockTickerData {
    private double price;
    private String symbol;

    public StockTickerData(String symbol, double price) {
        this.price = price;
        this.symbol = symbol;
    }

    public double getPrice() {
        return price;
    }

    public String getSymbol() {
        return symbol;
    }

    public void updatePrice() {
        this.price += Math.random() * 10;
    }

    @Override
    public String toString() {
        return String.format("%s: %.2f", symbol, price);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StockTickerData that = (StockTickerData) o;
        return Double.compare(that.price, price) == 0 &&
                symbol.equals(that.symbol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(price, symbol);
    }
}
