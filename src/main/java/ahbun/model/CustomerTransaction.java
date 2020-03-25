package ahbun.model;

public class CustomerTransaction {
    private String sessionInfo;
    private long totalShares = 0;
    private double totalPrice = 0;


    public CustomerTransaction update(StockTransaction stockTransaction) {
        this.totalShares = stockTransaction.shares;
        this.totalPrice = stockTransaction.sharePrice * this.totalShares;

        return this;
    }

    public CustomerTransaction reduce(CustomerTransaction customerTransaction) {
        this.totalShares += customerTransaction.totalShares;
        this.totalPrice += customerTransaction.totalPrice;
        return this;
    }

    public void setSessionInfo(String sessionInfo) {
        this.sessionInfo = sessionInfo;
    }

    public String toString() {
        return String.format("avg share price: %.2f",
                totalPrice / totalShares);
    }
}
