package ahbun.util;

public class PurchaseSimiluationRecipe {
    private final int maxCustomers;
    private final int maxEmployees;
    private final int iterations;

    public PurchaseSimiluationRecipe(int maxCustomers, int maxEmployees, int iterations) {
        this.maxCustomers = maxCustomers;
        this.maxEmployees = maxEmployees;
        this.iterations = iterations;
    }

    public int getMaxCustomers() {
        return maxCustomers;
    }

    public int getMaxEmployees() {
        return maxEmployees;
    }

    public int getIterations() {
        return iterations;
    }
}
