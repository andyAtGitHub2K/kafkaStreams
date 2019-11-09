package ahbun.chapter6.processor;

import ahbun.model.BeerDistribution;
import ahbun.model.Currency;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.To;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;

/***
 * BeerDistributionProcessor process the value by
 *    1. computing the DALLOR value of the sales
 *    2. direct the processed value to one of the two sinks (international or domestic) determined
 *       from the CURRENCY value found in the BeerDistribution.
 */
public class BeerDistributionProcessor extends AbstractProcessor<String, BeerDistribution> {
    private static Logger logger = LoggerFactory.getLogger(BeerDistributionProcessor.class);
    private final String internationalSalesSink;
    private final String domesticSalesSink;
    private static DecimalFormat decimalFormat = new DecimalFormat("###.##");

    public BeerDistributionProcessor(String internationalSalesSink, String domesticSalesSink) {
        this.internationalSalesSink = internationalSalesSink;
        this.domesticSalesSink = domesticSalesSink;
    }

    @Override
    public void process(String key, BeerDistribution value) {
        Currency valueCurrency = value.getCurrency();
        double sales;
        boolean domestic = false;

        if (valueCurrency == Currency.EUROS) {
            sales = Currency.EUROS.toUSD(value.getTotalSales());
        } else if (valueCurrency == Currency.POUNDS) {
            sales = Currency.POUNDS.toUSD(value.getTotalSales());
        } else {
            sales = value.getTotalSales();
            domestic = true;
        }

        // format sales number
        sales = Double.parseDouble(decimalFormat.format(sales));

        BeerDistribution beerDistribution =
                BeerDistribution.builder()
                .beerType(value.getBeerType())
                .currency(Currency.DOLLARS)
                .numberOfCases(value.getNumberOfCases())
                .totalSales(sales)
                .build();

        send(beerDistribution, domestic, key);
    }

    private void send(final BeerDistribution beerDistribution,
                      boolean isDomestic, String key) {
        // ProcessorContext, a context reference available through the init() method executed by the StreamTask.
        // during topology initialization.
        if (isDomestic) {
            logger.info("send to domestic\n" + beerDistribution.toString());
            context().forward(key, beerDistribution, To.child(domesticSalesSink));
        } else {
            logger.info("send to international\n" + beerDistribution.toString());
            context().forward(key, beerDistribution, To.child(internationalSalesSink));
        }
    }
}
