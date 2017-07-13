package de.cimt.talendcomp.jobinstance.jmx;

import java.io.IOException;

public interface TalendJobInfoMXBean {
	
	Integer getCounter(String component, String counterName) throws IOException;
	
	String getGlobalMapVars() throws IOException;
	
	String getContextVars() throws IOException;

	String getLogLevel() throws IOException;
	
	void setLogLevel(String level) throws IOException;
	
	long getJobInstanceId() throws IOException;
	
	String getTalendPid() throws IOException;
	
	String getErrorMessages() throws IOException;

}
