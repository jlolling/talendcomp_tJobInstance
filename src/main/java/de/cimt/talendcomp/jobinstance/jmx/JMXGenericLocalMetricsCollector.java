package de.cimt.talendcomp.jobinstance.jmx;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.Logger;

public class JMXGenericLocalMetricsCollector {

	private static final Logger LOG = Logger.getLogger(JMXGenericLocalMetricsCollector.class);
	private BaseClient baseClient = null;
	
	public JMXGenericLocalMetricsCollector(BaseClient baseClient, int pid) {
		this.pid = pid;
		this.baseClient = baseClient;
	}
	
	private int pid = 0;
	
	public void connect() throws Exception {
		JMXServiceURL url = new JMXServiceURL("service:jmx:attach:///" + pid);
		JMXConnector connector = JMXConnectorFactory.connect(url, null);
	}

	public int getPid() {
		return pid;
	}

	
	
}