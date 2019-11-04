package ahbun.chap3.zeemart;

import java.time.LocalDateTime;
import java.util.Date;

public interface SecurityDBService {
    static void saveRecord(LocalDateTime date, String employeeID, String item) {
        System.out.println("Security warning: " + date + ":" + employeeID + ":" + item);
    }
}
