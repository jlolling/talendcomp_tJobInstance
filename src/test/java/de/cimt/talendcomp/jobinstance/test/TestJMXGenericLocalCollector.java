package de.cimt.talendcomp.jobinstance.test;

import org.junit.Test;

import de.cimt.talendcomp.jobinstance.jmx.JMXGenericLocalMetricsCollector;

public class TestJMXGenericLocalCollector {
	
	@Test
	public void testConnectionLocal() throws Exception {
		JMXGenericLocalMetricsCollector c = new JMXGenericLocalMetricsCollector();
		c.setPid(28527);
		c.connect();
	}

}
