package ahbun.util;

import org.junit.Test;

import java.time.LocalDate;
import java.time.temporal.ChronoField;

import static org.junit.Assert.*;

public class NextWorkDayTest {

    @Test
    public void adjustInto() {
        LocalDate day1 = LocalDate.now();
        for (int i = 0; i < 7; i++) {
            LocalDate day2 = day1.with(new NextWorkDay());
            int dayOfTheWeek = day2.get(ChronoField.DAY_OF_WEEK);
            System.out.printf("%s %d\n", day2, dayOfTheWeek);
            day1 = day2;
        }
    }
}