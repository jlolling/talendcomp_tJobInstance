package de.cimt.talendcomp.jobinstance.jmx;

import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.Logger;

/**
 * JMX client
 * @author jan.lolling@gmail.com
 */
public class BaseClient {

	private static Logger logger = Logger.getLogger(BaseClient.class);
	private String jmxServiceUrl = null;
	private String jmxUser = null;
	private String jmxPassword = null;
	private MBeanServerConnection mBeanServerConnection = null;
	private JMXConnector jmxConnector = null;
	private long timeout = 1000l;
	private int maxRetryAttempts = 5; 
	
	public String getJmxServiceUrl() {
		return jmxServiceUrl;
	}
	
	public void setJmxServiceUrl(String jmxUrl) {
		this.jmxServiceUrl = jmxUrl;
	}
	
	public String getJmxUser() {
		return jmxUser;
	}
	
	public void setJmxUser(String jmxUser) {
		this.jmxUser = jmxUser;
	}
	
	public String getJmxPassword() {
		return jmxPassword;
	}
	
	public void setJmxPassword(String jmxPassword) {
		this.jmxPassword = jmxPassword;
	}
		
	public static boolean isEmpty(String s) {
		if (s == null) {
			return true;
		}
		if (s.trim().isEmpty()) {
			return true;
		}
		if (s.trim().equalsIgnoreCase("null")) {
			return true;
		}
		return false;
	}

	public MBeanServerConnection getmBeanServerConnection() {
		return mBeanServerConnection;
	}
	
	public void connect() throws Exception {
		logger.debug("Connect to " + jmxServiceUrl + "...");
		System.setProperty("sun.rmi.transport.connectionTimeout", String.valueOf(timeout));
		mBeanServerConnection = null;
        Map<String, String[]> environment = new HashMap<String, String[]>();
        environment.put(JMXConnector.CREDENTIALS, new String[] { jmxUser, jmxPassword });
        int currAttempts = 0;
        while (true) {
        	try {
        		currAttempts++;
        		jmxConnector = connectWithTimeout(new JMXServiceURL(jmxServiceUrl), environment, timeout);
        		break;
        	} catch (Exception e) {
        		if (currAttempts >= maxRetryAttempts) {
        			throw e;
        		} else {
        			logger.warn("Connect failed: " + e.getMessage() + ". Start retry #" + currAttempts);
        			Thread.sleep(1000);
        		}
        	}
        }
        mBeanServerConnection = jmxConnector.getMBeanServerConnection();
	}
	
	public void close() {
		if (jmxConnector != null) {
			try {
				jmxConnector.close();
			} catch (IOException e) {
				// ignore
			}
		}
		mBeanServerConnection = null;
	}
	
	private void checkConnection() throws Exception {
		if (mBeanServerConnection == null) {
			throw new Exception("Not connected. MBeanServerConnection is null.");
		}
	}
	
	public boolean isConnected() {
		return mBeanServerConnection != null;
	}
	
	public CompositeData getAttributeValue(String objectName, String attribute) throws Exception {
		checkConnection();
		return (CompositeData) mBeanServerConnection.getAttribute(new ObjectName(objectName), attribute);
	}
	
	@SuppressWarnings("unchecked")
	public Collection<CompositeData> getAttributeValues(String objectName, String attribute) throws Exception {
		checkConnection();
		TabularData tabularData = (TabularData) mBeanServerConnection.getAttribute(new ObjectName(objectName), attribute);
		if (tabularData != null) {
			return (Collection<CompositeData>) tabularData.values();
		} else {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public Collection<CompositeData> getAttributeCompositeValues(ObjectName objectName, String attribute) throws Exception {
		checkConnection();
		TabularData tabularData = (TabularData) mBeanServerConnection.getAttribute(objectName, attribute);
		return (Collection<CompositeData>) tabularData.values();
	}

	public Object getAttributeValue(ObjectName objectName, String attribute) throws Exception {
		checkConnection();
		return mBeanServerConnection.getAttribute(objectName, attribute);
	}

	public MemoryUsage getMemoryInfo() throws Exception {
		checkConnection();
		CompositeData cd = getAttributeValue("java.lang:type=Memory", "HeapMemoryUsage");
		return MemoryUsage.from(cd);
	}
	
	/*
	 * provides a timeout setting for JMX remote connects
	 */
	private static JMXConnector connectWithTimeout(final JMXServiceURL url, final Map<String, String[]> environment, long timeout) throws Exception { 
		final BlockingQueue<Object> mailbox = new ArrayBlockingQueue<Object>(1); 
		ExecutorService executor = Executors.newSingleThreadExecutor(); 
		executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					JMXConnector connector = JMXConnectorFactory.connect(url, environment);
					if (!mailbox.offer(connector)) {
						connector.close();
					}
				} catch (Throwable t) {
					mailbox.offer(t);
				}
			}
		}); 
		Object result = null; 
		try { 
			result = mailbox.poll(timeout, TimeUnit.MILLISECONDS); 
			if (result == null) { 
				if (!mailbox.offer("")) {
					result = mailbox.take(); 
				}
			} 
		} catch (InterruptedException e) {
			throw e; 
		} finally { 
			executor.shutdown(); 
		} 
		if (result == null) {
			throw new SocketTimeoutException("Connect timed out: " + url); 
		}
		if (result instanceof JMXConnector) {
			return (JMXConnector) result; 
		}
		try { 
			throw (Throwable) result; 
		} catch (IOException e) { 
			throw e; 
		} catch (RuntimeException e) { 
			throw e; 
		} catch (Error e) { 
			throw e; 
		} catch (Throwable e) { 
			// In principle this can't happen but we wrap it anyway 
			throw new IOException(e.toString(), e); 
		}
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

}
