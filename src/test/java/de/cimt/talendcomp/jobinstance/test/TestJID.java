package de.cimt.talendcomp.jobinstance.test;

import org.junit.Test;

import de.cimt.talendcomp.jobinstance.manage.JID;

public class TestJID {

	@Test
	public void testCreateJIDInBulk() throws Exception {
		JID jid = new JID();
		long lastId = 0;
		for (int t = 0; t < 1000; t++) {
			for (int i = 0; i < 100000; i++) {
				long id = jid.createJID();
				if (id <= lastId) {
					throw new Exception("Same or lower id found:" + id + " i: " + i + " lastId: " + lastId);
				}
				lastId = id;
			}
			System.out.println("Iteration: " + t + " lastId: " + lastId + " currentMillis: " + jid.getCurrentMillisecond() + " sequenceValue: " + jid.getSequenceValue());
			if (t % 2 == 0) {
				Thread.sleep(500);
			}
		}
	}
	    
}
