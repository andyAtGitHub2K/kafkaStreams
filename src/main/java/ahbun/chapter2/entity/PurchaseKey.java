package ahbun.chapter2.entity;

import java.util.Date;

public class PurchaseKey {
    private String clientId;
    private Date transactionDate;

    public PurchaseKey(String clientId, Date transactionDate) {
        this.clientId = clientId;
        this.transactionDate = transactionDate;
    }

    public String getClientId() {
        return clientId;
    }

    public Date getTransactionDate() {
        return transactionDate;
    }
}
