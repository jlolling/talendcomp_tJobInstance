package de.cimt.talendcomp.jobinstance.jmx;

import java.util.ArrayList;
import java.util.List;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

public class JMXCollectorControl {

	private BaseClient baseClient = null;
	private List<JMXMetric> currentMetrics = new ArrayList<JMXMetric>();
	private long lastExecutionTime = 0l;
	private long interval = 1000l;
	private ObjectName onMemory = new ObjectName("java.lang:type=Memory");
	private ObjectName onOperatingSystem = null;
	
	public JMXCollectorControl(BaseClient baseClient) throws MalformedObjectNameException {
		this.baseClient = baseClient;
		onMemory = new ObjectName("java.lang:type=Memory");
		onOperatingSystem = new ObjectName("java.lang:type=OperatingSystem");
	}
	
	private void collectJVMMetrics() throws Exception {
		CompositeData cdMemory = (CompositeData) baseClient.getAttributeValue(onMemory, "HeapMemoryUsage");
		JMXMetric metric = new JMXMetric();
		metric.setMetricName("heap_memory_used");
		Long memUsed = (Long) cdMemory.get("used");
		if (memUsed != null) {
			metric.setValue(memUsed.doubleValue());
		}
		currentMetrics.add(metric);
		metric = new JMXMetric();
		metric.setMetricName("heap_memory_max");
		Long memMax = (Long) cdMemory.get("max");
		if (memMax != null) {
			metric.setValue(memMax.doubleValue());
		}
		currentMetrics.add(metric);
		metric = new JMXMetric();
		metric.setMetricName("process_cpu_load");
		metric.setValue((Double) baseClient.getAttributeValue(onOperatingSystem, "ProcessCpuLoad"));
		currentMetrics.add(metric);
		metric = new JMXMetric();
		metric.setMetricName("system_cpu_load");
		metric.setValue((Double) baseClient.getAttributeValue(onOperatingSystem, "SystemCpuLoad"));
		currentMetrics.add(metric);
	}
	
	private void collectGenericMetrics() {
		
	}
	
	public JMXMetric next() {
		if (Thread.currentThread().isInterrupted()) {
			return null;
		}
		long now = System.currentTimeMillis();
		long diff = interval - (now - lastExecutionTime);
		if (diff > 0) {
			try {
				Thread.sleep(diff);
			} catch (InterruptedException e) {
				return null;
			}
		}
		lastExecutionTime = now;
		if (currentMetrics.isEmpty()) {
			try {
				collectJVMMetrics();
			} catch (Exception e) {
				
			}
			collectGenericMetrics();
		}
		if (currentMetrics.isEmpty()) {
			return null;
		}
		JMXMetric metric = currentMetrics.remove(0);
		if (currentMetrics.isEmpty()) {
			metric.setLastMetricInBatch(true);
		}
		return metric;
	}
		
}
