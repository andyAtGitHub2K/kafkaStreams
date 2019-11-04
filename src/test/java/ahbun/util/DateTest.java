package ahbun.util;
import com.github.javafaker.Faker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.*;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/***
 * Use LocalDate for most cases and ChronoLocalDate for localizing input/output of the program
 */
public class DateTest {
    private Faker faker = new Faker();

    @Before
    public void setup() {

    }

    @Test
    public void testCreateDateLocalDate(){
        // using LocalDate, LocalTime, LocalDateTime
        LocalDate date = LocalDate.of(2019, 10, 20);
        System.out.println(date.toString());

        int year = date.get(ChronoField.YEAR);
        int month= date.get(ChronoField.MONTH_OF_YEAR);
        int day = date.get(ChronoField.DAY_OF_WEEK);

        System.out.printf("year %d, month %d, day of the week %d\n", year, month, day);

        LocalDate now = LocalDate.now();
        System.out.println(now.toString());
        // Instant, Duration, Period
    }

    @Test
    public void testCreateDateLocalTime(){
        LocalTime time = LocalTime.of(13,12,20);
        LocalTime t2 = LocalTime.parse("13:12:20");
        LocalTime t3 = LocalTime.parse("13:12:21");
        Assert.assertTrue(time.equals(t2));
        Assert.assertFalse(time.equals(t3));
    }

    @Test
    public void testLocalDateTime(){
        LocalTime time = LocalTime.of(13,12,20);
        LocalDate date = LocalDate.of(2019, 10, 20);
        LocalDateTime dateTime = LocalDateTime.of(date, time);
        System.out.println(dateTime);

    }

    @Test
    public void testInstant(){
        // time for machine = seconds from Epoch time 1/1/1970 UTC
        Instant i = Instant.ofEpochSecond(10);
    }

    @Test
    public void testDuration() {
        // create Duration using Duration.between(2 times, dateTimes or Instants)
        // duration in seconds (eventually nano second)
        // not applicable for LocalDate
    }

    @Test
    public void testPeriod() {
        // use for finding the period between years, months and days
        Period tenDays = Period.between(LocalDate.of(10, 10, 1),
                LocalDate.of(10,10,11));
        System.out.println(tenDays.getDays());
        Assert.assertTrue(tenDays.getDays() == 10);
    }

    @Test
    public void testLocalDateWithMethod() {
        LocalDate date1 = LocalDate.of(2019, 10, 2);
        LocalDate date2 = date1.withYear(2020);
        LocalDate date3 = date1.withDayOfMonth(10);
        LocalDate date4 = date1.with(ChronoField.YEAR, 2015);
        System.out.println(date1);
        System.out.println(date2);
        System.out.println(date3);
        System.out.println(date4);
    }

    @Test
    public void testPrintAndParse() {
         // DateTimeFormatter.ofPattern(...)
        // LocalDate.parse(formattedDate, formatter)
    }

    @Test
    public void testTimeZone() {
        // java.time.ZoneId replaces java.util.TimeZone
        // region IDs are in the format "{area}/{city}"
        // Internet Assigned Numbers Authority https://www.iana.org/time-zones

        // Converts old TimeZone to ZoneId
        ZoneId zoneId = TimeZone.getDefault().toZoneId();
        System.out.println(zoneId.getId());
        // using ZoneId + (LocalDate, LocalDateTime) => ZoneDateTime
        Instant instant = Instant.now();
        ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        System.out.println(zonedDateTime);

        // converting between LocalDateTime -> Instant
        LocalDateTime localDateTime = LocalDateTime.of(2019, 10, 22, 10, 0, 0);
        // get offset from zondId
        ZoneOffset offset = zoneId.getRules().getOffset(localDateTime);
        System.out.printf("getTotalSeconds: %d", offset.getTotalSeconds());
        Instant instant1 = localDateTime.toInstant(offset); // 2019-10-22T17:00:00Z
        System.out.printf("Instant : %s\n", instant1.toString());
        Assert.assertTrue("2019-10-22T17:00:00Z".equals(instant1.toString()));
        Instant instant2 = Instant.parse("2019-10-22T17:00:00Z");
        Assert.assertTrue(instant1.equals(instant2));

        ZoneId zoneId1 = ZoneId.of("America/New_York");

        Instant instant3 = localDateTime.toInstant(zoneId1.getRules().getOffset(localDateTime));
        Assert.assertFalse(instant2.equals(instant3));
        Assert.assertTrue(instant2.compareTo(instant3) > 0);

        // Instant -> LocalDateTime
        LocalDateTime localDateTime1 = LocalDateTime.ofInstant(instant2, zoneId);
        Assert.assertTrue(localDateTime1.equals(localDateTime));
        System.out.printf("Instant 3 : %s\n", instant3.toString());
    }

    @Test
    public void testFakeTime() {
        Supplier<Date> timestampGenerator = () -> faker.date().past(45, TimeUnit.MINUTES, new Date());
        ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        for (int i = 0; i < 10; i++) {
            Date date = timestampGenerator.get();
            LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), zoneId);
           System.out.println(localDateTime.toString());
        }
    }
}
