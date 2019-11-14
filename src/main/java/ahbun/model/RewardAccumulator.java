package ahbun.model;

import java.util.Objects;

/***
 * Reward computes the reward points of the customer purchase.
 * Reward rules: each dollar worth one reward point. Total purchase is
 * round down to the nearest dollar.
 */
public class RewardAccumulator {
    private String custumerID;
    private double purchaseTotal;
    private int currentRewardPoints;
    private int daysFromLastPurchase; // new fields
    private int totalPurchasePoint;   // new fields

 

    private RewardAccumulator(String custumerID, double purchaseTotal, int rewardPoints) {
        this.custumerID = custumerID;
        this.purchaseTotal = purchaseTotal;
        this.currentRewardPoints = rewardPoints;
        this.totalPurchasePoint = rewardPoints;
    }

    public static RewardBuilder builder(Purchase purchase) { return new RewardBuilder(purchase); }

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

    public int getCurrentRewardPoints() {
        return currentRewardPoints;
    }
    public void addRewardPoints(int points) {
        this.totalPurchasePoint += points;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RewardAccumulator that = (RewardAccumulator) o;
        return Double.compare(that.purchaseTotal, purchaseTotal) == 0 &&
                currentRewardPoints == that.currentRewardPoints &&
                daysFromLastPurchase == that.daysFromLastPurchase &&
                totalPurchasePoint == that.totalPurchasePoint &&
                Objects.equals(custumerID, that.custumerID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(custumerID, purchaseTotal, currentRewardPoints, daysFromLastPurchase, totalPurchasePoint);
    }


    public static class RewardBuilder {
        private String custumerID;
        private double purchaseTotal;
        private int rewardPoints;
        private int daysFromLastPurchase;

        private RewardBuilder(Purchase purchase) {
            this.custumerID = purchase.getLastName() + "," + purchase.getFirstName() ;
            this.purchaseTotal = (double)purchase.getQuantity() * purchase.getPrice();
            this.rewardPoints = (int)purchaseTotal;
        }

        public RewardAccumulator build() { return new RewardAccumulator(custumerID, purchaseTotal, rewardPoints);}

        public String getCustumerID() {
            return custumerID;
        }

        public double getPurchaseTotal() {
            return purchaseTotal;
        }

        public int getRewardPoints() {
            return rewardPoints;
        }

        public int getDaysFromLastPurchase() {
            return daysFromLastPurchase;
        }


    }
 }
