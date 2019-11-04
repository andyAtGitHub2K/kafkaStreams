package ahbun.model;

import java.util.Objects;

/***
 * Reward computes the reward points of the customer purchase.
 */
public class Reward {
    private String custumerID;
    private double purchaseTotal;
    private int totalPurchasePoint;
    private int daysFromLastPurchase;


    private Reward(RewardBuilder rb) {
        this.custumerID = rb.custumerID;
        this.purchaseTotal = rb.purchaseTotal;
        this.totalPurchasePoint = rb.totalPurchasePoint;
    }

    public static RewardBuilder builder() { return new RewardBuilder(); }

    public static RewardBuilder builder(Purchase purchase) {
        RewardBuilder rewardBuilder = builder();

        rewardBuilder.custumerID = purchase.getCustomerId();
        rewardBuilder.purchaseTotal = purchase.getPrice() * purchase.getQuantity();
        rewardBuilder.totalPurchasePoint = (int)rewardBuilder.purchaseTotal;
        return rewardBuilder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Reward reward = (Reward) o;
        return Double.compare(reward.purchaseTotal, purchaseTotal) == 0 &&
                totalPurchasePoint == reward.totalPurchasePoint &&
                daysFromLastPurchase == reward.daysFromLastPurchase &&
                Objects.equals(custumerID, reward.custumerID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(custumerID, purchaseTotal, totalPurchasePoint, daysFromLastPurchase);
    }

    public String getCustumerID() {
        return custumerID;
    }

    public double getPurchaseTotal() {
        return purchaseTotal;
    }

    public int getTotalPurchasePoint() {
        return totalPurchasePoint;
    }

    public int getDaysFromLastPurchase() {
        return daysFromLastPurchase;
    }

    @Override
    public String toString() {
        return "Reward{" +
                "custumerID='" + custumerID + '\'' +
                ", purchaseTotal=" + purchaseTotal +
                ", totalPurchasePoint=" + totalPurchasePoint +
                ", daysFromLastPurchase=" + daysFromLastPurchase +
                '}';
    }

    public static class RewardBuilder {
        private String custumerID;
        private double purchaseTotal;
        private int totalPurchasePoint;
        private int daysFromLastPurchase;

        public Reward build() { return new Reward(this);}

        public String getCustumerID() {
            return custumerID;
        }

        public double getPurchaseTotal() {
            return purchaseTotal;
        }

        public int getTotalPurchasePoint() {
            return totalPurchasePoint;
        }

        public int getDaysFromLastPurchase() {
            return daysFromLastPurchase;
        }
    }
 }
