package de.cimt.talendcomp.jobinstance.jmx;

public class JMXMetric {
	
	private Double value = null;
	private String metricName = null;
	private boolean isLastMetricInBatch = false;
	
	public Double getValue() {
		return value;
	}
	public void setValue(Double value) {
		this.value = value;
	}
	public String getMetricName() {
		return metricName;
	}
	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}
	public boolean isLastMetricInBatch() {
		return isLastMetricInBatch;
	}
	public void setLastMetricInBatch(boolean isLastMetricInBatch) {
		this.isLastMetricInBatch = isLastMetricInBatch;
	}

}
