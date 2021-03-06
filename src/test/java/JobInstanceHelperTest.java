import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


public class JobInstanceHelperTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//test_changeTimezone();
		//test_bundle();
		String s1 = "57156502";
		String s2 = "106399905";
		System.out.println(s1.compareTo(s2));
	}
	
    private static void test_changeTimezone() {
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date before = new Date();
        System.out.println("Before formatted: " + sdf1.format(before));
        // interne Zeit ist immer UTC
        System.out.println("Before long: " + before.getTime());
        try {
             // Text Parsen berücksichtigt die Zeitzone
             System.out.println("Before parsed: " + sdf1.parse("2014-05-22 08:30:00").getTime());
        } catch (ParseException e) {
	        e.printStackTrace();
        }
        if ("UTC".equals(TimeZone.getDefault().getID()) == false) {
        	System.out.println("Change default time zone to UTC");
        	TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        }
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date after = new Date();
        System.out.println("After formatted: " + sdf2.format(after));
        // interne Zeit ist immer UTC
        System.out.println("After long: " + after.getTime());
        try {
	        // Text Parsen berücksichtigt die Zeitzone
            System.out.println("After parsed: " + sdf2.parse("2014-05-22 08:30:00").getTime());
        } catch (ParseException e) {
	        e.printStackTrace();
        }
    }
	
	private static String limitMessage(String message, int size, int cutPosition) {
		if (message != null && message.trim().isEmpty() == false) {
			message = message.trim();
			if (message.length() > size) {
				size = size - 3; // to have space for "..."
				if (cutPosition == 0) {
					return message.substring(0, size) + "...";
				} else if (cutPosition == 1) {
					StringBuilder sb = new StringBuilder();
					sb.append(message.substring(0, size / 2));
					sb.append("...");
					sb.append(message.substring(message.length() - size / 2, message.length()));
					return sb.toString();
				} else {
					return "..." + message.substring(message.length() - size);
				}
			} else {
				return message;
			}
		} else {
			return null;
		}
	}
	
	private static Connection createMySQLConnection() throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://on-0337-jll.local:3306/managed_test", "tisadmin", "tisadmin");
		return conn;
	}
	
}
