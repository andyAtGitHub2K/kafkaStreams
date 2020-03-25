package ahbun.chapter9;

import ahbun.util.MockDataProducer;

import java.io.IOException;
import java.time.LocalDateTime;

public class MockInteractiveDataProducer {
    public static void main(String[] argv) throws IOException {
        // setup
        String sourceTopic = "interactive-source-topic";
        LocalDateTime localDateTime = LocalDateTime.of(2019, 10, 22, 10, 0, 0);
        String[] customerIDList =  {"12345678", "222333444", "33311111", "55556666", "4488990011", "77777799", "111188886","98765432", "665552228", "660309116"};
        String[]  symbols = {"AEBB", "VABC", "ALBC", "EABC", "BWBC",
                "BNBC", "MASH", "BARX", "WNBC", "WKRP"};
        String[] sector = {"Energy", "Finance", "Technology", "Transportation", "Health Care",
                "Energy", "Finance", "Technology", "Transportation", "Health Care"};
        String[] industry = {"OilGasPro", "FinInServices", "CoalMining", "Railroads", "MajorPharm",
                "Mining",  "Banking", "ComComEq","Aerospace", "Nursing" };
        IQConfig iqConfig = new IQConfig(customerIDList, industry, sector, symbols, localDateTime, 5);
        int[] windowSizeInSeconds ={5, 10, 14};
        int[] windowGapsInSeconds = {2, 5, 7};

        // generate data
        MockDataProducer.produceTxWithinWindowsForIQ(sourceTopic,
                5, 2, 5,
                windowSizeInSeconds, windowGapsInSeconds,null, iqConfig
        );


        Runtime.getRuntime().addShutdownHook(new Thread(MockDataProducer::shutdown));
    }
}
