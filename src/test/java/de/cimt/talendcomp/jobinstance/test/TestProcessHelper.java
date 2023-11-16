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
		List<Integer> pidList = ph.retrieveProcessListForUnix();
		System.out.println("Found: " + pidList.size() + " processes");
		assertTrue("Empty list", pidList.size() > 0);
	}

}
