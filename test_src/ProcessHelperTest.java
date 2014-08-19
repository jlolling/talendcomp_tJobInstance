import java.text.SimpleDateFormat;
import java.util.List;

import de.cimt.talendcomp.process.ProcessHelper;


public class ProcessHelperTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat();
		System.out.println(sdf.toLocalizedPattern());
		ProcessHelper ph = new ProcessHelper();
		try {
			ph.init();
			List<Integer> pids = ph.retrieveProcessList();
			for (Integer pid : pids) {
				System.out.println(pid);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		

	}

}
