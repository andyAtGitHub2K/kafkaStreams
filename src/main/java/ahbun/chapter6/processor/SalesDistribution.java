package ahbun.chapter6.processor;

import org.apache.kafka.streams.Topology;

import java.util.Properties;

/***
 * Using Processor API to build Stream Topology
 *
 * 1. consume sales and separate domestic and international sales to two different topics
 *
 *    models: Beer Purchase - Currency, totalSales, numberOfCases, beerType
 */
public class SalesDistribution {
    public static void main(String[] args) {

    }

    private static Topology buildTopology(Properties appProperties) {
        return null;
    }
}
