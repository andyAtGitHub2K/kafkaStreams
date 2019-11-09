package ahbun.model;

import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayDeque;

/***
 * StockPerformance computes stock trend indicators :
 *      up and down price/volume indicator
 * based on N-sampled price/volume.
 */
public class StockPerformance {
    private static DecimalFormat decimalFormat = new DecimalFormat("###.##");

    Instant lastUpdateInstant;
    Instant sendMessageInstant;
    private final int MAX_QUEUE_SIZE;
    private double currentPrice = 0.0;
    private double currentAvgPrice = Double.MIN_VALUE;
    private double priceChangePCT = 0.0;
    private ArrayDeque<Double> priceHistory;//= new ArrayDeque<>(MAX_QUEUE_SIZE);

    private double currentVolume = 0.;
    private double currentAvgVolume = Double.MIN_VALUE;
    private double volumeChangePCT = 0.;
    private ArrayDeque<Double> volumeHistory;// = new ArrayDeque<>(MAX_QUEUE_SIZE);

    public StockPerformance() {
        this(0);
    }

    public StockPerformance(int queueSize) {
        if (queueSize <= 0) {
            queueSize = 5;
        }
        MAX_QUEUE_SIZE = queueSize;
        priceHistory = new ArrayDeque<>(MAX_QUEUE_SIZE);
        volumeHistory = new ArrayDeque<>(MAX_QUEUE_SIZE);
    }

    public void update(double newPrice, int newVolume) {
        updatePriceStats(newPrice);
        updateVolumeStats(newVolume);
        lastUpdateInstant = Instant.now();
    }

    private void updateVolumeStats(int newVolume) {
        this.currentVolume = newVolume;
        this.currentAvgVolume = Double.parseDouble(decimalFormat.format(computeSMA(newVolume, currentAvgVolume, volumeHistory)));
        this.volumeChangePCT = Double.parseDouble(decimalFormat.format(computeDifferentialPCT(newVolume, currentAvgVolume)));
    }

    /***
     * Update the price statistics related to current, average and relative change wrt average.
     * @param newPrice new price
     */
    private void updatePriceStats(double newPrice) {
        this.currentPrice = newPrice;
        this.currentAvgPrice = Double.parseDouble(decimalFormat.format(computeSMA(newPrice, currentAvgPrice, priceHistory)));
        this.priceChangePCT = Double.parseDouble(decimalFormat.format(computeDifferentialPCT(newPrice, currentAvgPrice)));
    }

    /***
     * Calculates the percentage change of the given value against the average.
     * @param currentValue current value
     * @param average average value
     * @return 0 until the first N-sample average is available for the computation.
     */
    private double computeDifferentialPCT(double currentValue, double average) {
        return average <= Double.MIN_VALUE ? 0.0 : (currentValue / average - 1) * 100.0;
    }

    /***
     * Computes a simple moving average over N-sampled data {@code MAX_QUEUE_SIZE}
     * @param newValue new value
     * @return a simple moving average of the N-sampled data.
     */
    private double computeSMA(double newValue, double currentAverage, ArrayDeque<Double> deque) {
        // when the current q size is < max
        if (deque.size() < MAX_QUEUE_SIZE) {
            // add the number to the q
            deque.add(newValue);

            // compute the average when the N-sample is collected
            if (deque.size() == MAX_QUEUE_SIZE) {
                currentAverage = deque.stream().reduce(0.0, Double::sum) / MAX_QUEUE_SIZE;
            }
        } else {
            // queue is filled up
            double oldestValue = deque.poll();
            deque.add(newValue);
            currentAverage = currentAverage + (newValue - oldestValue) / MAX_QUEUE_SIZE;
        }

        return currentAverage;
    }

    public Instant getLastUpdateInstant() {
        return lastUpdateInstant;
    }

    public double getCurrentPrice() {
        return currentPrice;
    }

    public double getCurrentAvgPrice() {
        return currentAvgPrice;
    }

    public double getPriceChangePCT() {
        return priceChangePCT;
    }

    public double getCurrentVolume() {
        return currentVolume;
    }

    public double getCurrentAvgVolume() {
        return currentAvgVolume;
    }

    public double getVolumeChangePCT() {
        return volumeChangePCT;
    }

    public Instant getSendMessageInstant() {
        return sendMessageInstant;
    }

    public void sendMessageInstant(Instant sendMessageInstant) {
        this.sendMessageInstant = sendMessageInstant;
    }

    @Override
    public String toString() {
        return "StockPerformance{" +
                "lastUpdateInstant=" + lastUpdateInstant +
                ", sendMessageInstant=" + sendMessageInstant +
                ", priceChangePCT=" + priceChangePCT +
                ", volumeChangePCT=" + volumeChangePCT +
                '}';
    }
}
