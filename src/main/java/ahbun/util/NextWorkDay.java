package ahbun.util;

import java.time.temporal.*;

public class NextWorkDay implements TemporalAdjuster {
    @Override
    public Temporal adjustInto(Temporal temporal) {
        int day = temporal.get(ChronoField.DAY_OF_WEEK);
        if (day > 4) {
            return temporal.plus(8 - day, ChronoUnit.DAYS);
        }

        return temporal.plus(1, ChronoUnit.DAYS);
    }
}
