package ahbun.model;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StockPerformanceTest {
    private StockPerformance stockPerformance;
    private int volume[] = {100, 200, 300, 400, 500};
    private double price[] = {10.0, 20.0, 30.0, 40.0, 50.0};

    private double expectedAvgPrice[] = {0.0,0.0,20.0,20 + (40-10)/3, 30 + (50-20)/3};
    private double expectedChangePCT[] = {0.,0.,50.,33.33,25.};

    @Before
    public void setup() {
        stockPerformance = new StockPerformance(3);
    }

    @Test
    public void update() {
        for (int i = 0; i < volume.length; i++ ) {
            stockPerformance.update(price[i], volume[i]);
            Assert.assertTrue(Math.abs(expectedAvgPrice[i] - stockPerformance.getCurrentAvgPrice()) < 0.0001);
            Assert.assertTrue(Math.abs(expectedAvgPrice[i]* 10 - stockPerformance.getCurrentAvgVolume()) < 0.0001);
            Assert.assertTrue(Math.abs(expectedChangePCT[i] - stockPerformance.getPriceChangePCT()) < 0.001);
            Assert.assertTrue(Math.abs(expectedChangePCT[i] - stockPerformance.getVolumeChangePCT()) < 0.001);
        }
    }
}