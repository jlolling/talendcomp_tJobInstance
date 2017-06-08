package de.cimt.talendcomp.jobinstance.test;

import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Test;

import de.cimt.talendcomp.jobinstance.manage.JID1;

public class TestJID1 {

	@Test
	public void testUniqueTimeMillis() throws Exception {
		long startTime = System.currentTimeMillis();
		long endTime = startTime + (300 * 1000l);
		long now = 0;
		long now2 = 0;
		long iterations = 0;
		while (true) {
			now = System.currentTimeMillis();
			Thread.sleep(1);
			now2 = System.currentTimeMillis();
			if (now == now2) {
				throw new Exception("Found equal unix time between a milli second: now=" + now);
			} else if (now2 >= endTime) {
				break;
			}
			iterations++;
		}
		System.out.println("Test successfull. Number iterations: " + iterations + " and duration: " + (endTime - startTime));
		assertTrue(true);
	}
	
	@Test
	public void testJID() throws Exception {
		JID1 jid1 = new JID1();
		jid1.setHostIndex((short) 123);
		jid1.setUnixPid(555);
		long lastId = 0;
		for (int i = 0; i < 10000; i++) {
			long startDate = new Date().getTime();
//			System.out.println("startDate=" + startDate + " startDate & mask33=" + (startDate & JID.mask33));
			jid1.setStartDate(startDate);
			long id = jid1.createJID();
			if (id <= lastId) {
				throw new Exception("fall back id found: " + id + " lastId: " + lastId);
			}
//			System.out.println(id + " time-part: " + jid.getTimePart(jid.getJID()) + " host-index=" + jid.getHostIndex(jid.getJID()) + " pid=" + jid.getUnixPid(jid.getJID()));
			lastId = id;
			Thread.sleep(1);
		}
	}
	
}
