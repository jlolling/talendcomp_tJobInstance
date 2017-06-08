package de.cimt.talendcomp.jobinstance.test;

import java.util.Random;

import org.junit.Test;

import de.cimt.talendcomp.jobinstance.manage.JID2;

public class TestJID2 {

	@Test
	public void testJID2() throws Exception {
		JID2 jid = new JID2();
		long lastId = 0;
		for (int t = 0; t < 10; t++) {
			for (int i = 0; i < 100000; i++) {
				long id = jid.createJID();
				if (id <= lastId) {
					throw new Exception("Same or lower id found:" + id + " i: " + i + " lastId: " + lastId);
				}
				lastId = id;
			}
			if (t % 2 == 0) {
				Thread.sleep(500);
			}
		}
	}
	
    public static String getAsciiRandomString(int length) {
        Random random = new Random();
        int cnt = 0;
        StringBuilder buffer = new StringBuilder();
        char ch;
        int end = 'z' + 1;
        int start = ' ';
        while (cnt < length) {
            ch = (char) (random.nextInt(end - start) + start);
            if (Character.isLetterOrDigit(ch)) {
                buffer.append(ch);
                cnt++;
            }
        }
        return buffer.toString();
    }
    
}
