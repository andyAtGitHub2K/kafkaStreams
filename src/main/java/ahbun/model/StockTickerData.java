package ahbun.model;

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
}
