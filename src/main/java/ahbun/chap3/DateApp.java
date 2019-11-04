package ahbun.chap3;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
public class DateApp {
    public static void main(String[] args) throws ParseException {
        String date = "2013-03-11T01:38:18.309Z";
        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        Date dd = d.parse(date);
        System.out.println(dd.toString());
    }
}
