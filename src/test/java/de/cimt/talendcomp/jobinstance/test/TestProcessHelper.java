package de.cimt.talendcomp.jobinstance.test;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import de.cimt.talendcomp.jobinstance.process.ProcessHelper;

public class TestProcessHelper {

	@Test
	public void testListProcessesUnix() throws Exception {
		ProcessHelper ph = new ProcessHelper();
		ph.init();
		List<Integer> pidList = ph.retrieveProcessList();
		System.out.println("Found: " + pidList.size() + " processes");
		assertTrue("Empty list", pidList.size() > 0);
	}

	@Test
	public void testListProcessesUnixAltCommand() throws Exception {
		ProcessHelper ph = new ProcessHelper();
		ph.init();
		ph.setUnixCommand("ps -eo pid");
		ph.setUnixPidPattern("([0-9]{1,9})");
		List<Integer> pidList = ph.retrieveProcessList();
		System.out.println("Found: " + pidList.size() + " processes");
		assertTrue("Empty list", pidList.size() > 0);
	}

	@Test
	public void testListProcessesUnixAltCommandErrorHandling() throws Exception {
		ProcessHelper ph = new ProcessHelper();
		ph.init();
		ph.setUnixCommand("xyz abc");
		try {
			ph.retrieveProcessList();
			assertTrue(false);
		} catch (Exception e) {
			String m = e.getMessage();
			System.out.println(m);
			assertTrue(m != null && m.contains("failed"));
		}
	}

}
