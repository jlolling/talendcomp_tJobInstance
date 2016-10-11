/**
 * Copyright 2015 Jan Lolling jan.lolling@gmail.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.cimt.talendcomp.jobinstance.log4j;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.varia.NullAppender;
import org.apache.log4j.xml.DOMConfigurator;

public class Log4JHelper {
	
	private static Map<String, Log4JHelper> cache = new ConcurrentHashMap<String, Log4JHelper>();
	private Object monitorLogging = new Object();
	private static boolean log4jInitialized = false;
	private static String log4jConfigFile;
	private long jobInstanceId = 0;
	private String workItem = null;
	private long processInstanceId = 0;
	public static String rootLoggerName = "talend";
	private String jobName;
	private String project;
	private String context;
	private String talendPid;
	private String talendFatherPid;
	private String talendRootPid;
	private Logger jobLogger = null;
	private Logger talendRoot = null;
	private String loggerNamePattern = null;
	private TalendJobFileAppender fileAppender;
	private boolean debug = true;
	private long jobStartedAt = 0;
	public static final String KEY_START_TS_LONG = "jobStartTimestampLong";
	public static final String KEY_START_TS_COMPACT = "jobStartTimestampCompact";
	public static final String KEY_START_DATE_LONG = "jobStartDateLong";
	public static final String KEY_START_DATE_COMPACT = "jobStartDateCompact";
	public static final String KEY_WORK_ITEM = "workItem";
	private SimpleDateFormat sdfTsLong = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private SimpleDateFormat sdfTsCompact = new SimpleDateFormat("yyyyMMdd_HHmmss.SSS");
	private SimpleDateFormat sdfDayLong = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat sdfDayCompact = new SimpleDateFormat("yyyyMMdd");
	private static boolean stdOutRedirected = false;
	private String host = null;
	private String user = null;
	private int pid = 0;
	
	public Log4JHelper() {
		retrieveProcessInfo();
	}
	
	public static void put(String key, Log4JHelper h) {
		cache.put(key, h);
	}
	
	public static Log4JHelper get(String key) {
		return cache.get(key);
	}
	
	public static boolean isLog4JInitialized() {
		return log4jInitialized;
	}
	
	public static void setupConsoleAppenderToRoot(String pattern) {
		Enumeration<Appender> enumApp = Logger.getRootLogger().getAllAppenders();
		boolean hasConsoleAppender = false;
		while (enumApp.hasMoreElements()) {
			Appender app = enumApp.nextElement();
			if (app instanceof ConsoleAppender) {
				hasConsoleAppender = true;
			}
		}
		if (hasConsoleAppender == false) {
			if (pattern != null && pattern.isEmpty() == false) {
				Logger.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout(pattern.trim())));
			} else {
				Logger.getRootLogger().addAppender(new ConsoleAppender());
			}
		}
	}
	
	/**
	 * file log4j.properties or log4j.xml or given file will be searched in:
	 * in given logConfigFileDir
	 * as resource
	 */
	public static void initLog4J() throws Exception {
		synchronized(monitorConfig) {
			if (log4jInitialized == false) {
				log4jInitialized = true;
				// create a minimum basic configuration to get the first logs
				Logger.getRootLogger().setLevel(Level.INFO);
				if (Logger.getRootLogger().getAllAppenders().hasMoreElements() == false) {
					// root needs always an appender to avoid collecting messages
					Logger.getRootLogger().addAppender(new NullAppender());
				}
				if (log4jConfigFile != null) {
					try {
						// find the log4j configuration and configure it
						File cf = new File(log4jConfigFile);
						if (cf.exists()) {
							if (log4jConfigFile.endsWith(".xml")) {
								DOMConfigurator.configureAndWatch(cf.getAbsolutePath(), 10000);
							} else if (log4jConfigFile.endsWith(".properties")) {
								PropertyConfigurator.configureAndWatch(cf.getAbsolutePath(), 10000);
							} else {
								log4jInitialized = false;
								throw new Exception("Unknown log4j file format:" + log4jConfigFile);
							}
						}
					} catch (Throwable e) {
						throw new Exception(e);
					}
				}
			}
		}
	}
	
	private String getJobLoggerName() {
		return rootLoggerName + "." + project + "." + jobName;
	}
	
	public void initJobLogger() {
		if ("root".equals(rootLoggerName)) {
			talendRoot = Logger.getRootLogger();
		} else {
			talendRoot = Logger.getLogger(rootLoggerName);
		}
		talendRoot.setAdditivity(true);
		talendRoot.setLevel(Level.INFO);
		jobLogger = Logger.getLogger(getJobLoggerName());
	}
	
	public void setJobLogger(Logger logger) {
		if (logger != null) {
			this.jobLogger = logger;
		}
	}
	
	private void retrieveProcessInfo() {
		String processInfo = ManagementFactory.getRuntimeMXBean().getName();
		int p = processInfo.indexOf('@');
		if (p > 0) {
			pid = Integer.valueOf(processInfo.substring(0, p));
			MDC.put("pid", pid);
			host = processInfo.substring(p + 1);
			MDC.put("host", host);
		} else {
			host = processInfo;
			MDC.put("host", host);
		}
		user = System.getProperty("user.name");
		MDC.put("user", user);
	}
	
	public String configureFileAppender(String logFileNamePattern, String patternLayout) {
		if (logFileNamePattern == null || logFileNamePattern.trim().isEmpty()) {
			throw new IllegalArgumentException("logFileNamePattern cannot be null or empty");
		}
		if (patternLayout == null || patternLayout.trim().isEmpty()) {
			throw new IllegalArgumentException("logFileNamePattern cannot be null or empty");
		}
		TalendJobFileAppender appender = new TalendJobFileAppender();
		appender.setAppend(true);
		appender.setImmediateFlush(true);
		if (jobInstanceId > 0) {
			appender.setJobAttribute("jobInstanceId", jobInstanceId);
		}
		appender.setJobAttribute("talendPid", talendPid);
		appender.setJobAttribute("talendFatherPid", talendFatherPid);
		appender.setJobAttribute("talendRootPid", talendRootPid);
		appender.setJobAttribute("jobName", jobName);
		appender.setJobAttribute("project", project);
		appender.setJobAttribute("context", context);
		appender.setJobAttribute(KEY_WORK_ITEM, workItem);
		appender.setJobAttribute(KEY_START_TS_COMPACT, MDC.get(KEY_START_TS_COMPACT));
		appender.setJobAttribute(KEY_START_TS_LONG, MDC.get(KEY_START_TS_LONG));
		appender.setJobAttribute(KEY_START_DATE_COMPACT, MDC.get(KEY_START_DATE_COMPACT));
		appender.setJobAttribute(KEY_START_DATE_LONG, MDC.get(KEY_START_DATE_LONG));
		appender.setFile(logFileNamePattern.trim());
		appender.setAppend(true);
		appender.setEncoding("UTF-8");
		appender.setLayout(new PatternLayout(patternLayout.trim()));
		synchronized(monitorConfig) {
			if (talendRoot == null) {
				throw new IllegalArgumentException("talendRoot logger not initialized. Please call initJobLogger() before!");
			}
			if (talendRoot.getAppender(appender.getName()) == null) {
				appender.activateOptions();
				if (jobLogger == null) {
					throw new IllegalStateException("jobLogger not initialized. Please call initJobLogger() before or set the jobLogger!");
				}
				jobLogger.setAdditivity(true);
				jobLogger.setLevel(Level.INFO);
				fileAppender = appender;
				talendRoot.addAppender(fileAppender);
			} else {
				fileAppender = null;
			}
		}
		return appender.getFile();
	}
	
	public Appender getFileAppender() {
		return fileAppender;
	}
	
	public static String printOutLoggers() {
		synchronized (monitorConfig) {
			StringBuilder sb = new StringBuilder();
			sb.append("###########################################\n");
			List<Logger> loggerList = new ArrayList<Logger>();
	        Logger rootLogger = Logger.getRootLogger();
	        LoggerRepository rep = rootLogger.getLoggerRepository();
	        for (@SuppressWarnings("unchecked")
			Enumeration<Logger> el = rep.getCurrentLoggers(); el.hasMoreElements(); ) {
	            loggerList.add(el.nextElement());
	        }
	        Collections.sort(loggerList, new Comparator<Logger>() {

				@Override
				public int compare(Logger o1, Logger o2) {
					return o1.getName().compareTo(o2.getName());
				}
	        	
			});
	        loggerList.add(0, rootLogger);
	        for (Logger l : loggerList) {
	        	sb.append(l.getName());
	        	if (l == Logger.getRootLogger()) {
	        		sb.append(" is root");
	        	}
	        	sb.append("\n");
	        	sb.append(printOutAppenders(l));
	        }
	        sb.append("\n##########################################\n");
	        String result = sb.toString();
	        System.out.println(result);
	        return result;
		}
	}
	
	private static String printOutAppenders(Logger logger) {
		StringBuilder sb = new StringBuilder();
		@SuppressWarnings("unchecked")
		Enumeration<Appender> en = logger.getAllAppenders();
		while (en.hasMoreElements()) {
			Appender a = en.nextElement();
			String name = a.getName();
			if (name == null) {
				name = "class:" + a.getClass().getName();
			}
			if (a instanceof FileAppender) {
				sb.append("    " + name + " file:" + ((FileAppender) a).getFile());
	        	sb.append("\n");
			} else {
				sb.append("    " + name);
	        	sb.append("\n");
			}
		}
		return sb.toString();
	}
	
	public Logger getJobLogger() {
		return jobLogger;
	}

	public void logOut(
			String partName,
			String logPriority,
			String message,
			String pid,
			Long jobInstanceId) {
		int priority = 0;
		if ("FATAL".equals(logPriority)) {
			priority = 6;
		} else if ("ERROR".equals(logPriority)) {
			priority = 5;
		} else if ("WARN".equals(logPriority)) {
			priority = 4;
		} else if ("INFO".equals(logPriority)) {
			priority = 3;
		} else if ("DEBUG".equals(logPriority)) {
			priority = 2;
		} else if ("TRACE".equals(logPriority)) {
			priority = 1;
		}
		String origin = partName;
		if (partName.startsWith("NODE:")) {
			origin = partName.substring("NODE:".length());
		}
		int pos = origin.lastIndexOf('_');
		String type = null;
		if (pos > 0) {
			type = origin.substring(0, pos);
		}
		logOut(new Date(), priority, type, origin, message, null, pid, jobInstanceId);
	}
	
	/**
	 * interface method for the component tLogCatcher
	 * @param moment
	 * @param priority
	 * @param type
	 * @param origin
	 * @param message
	 * @param code
	 */
	public void logOut(
			Date moment,
			Integer priority,
			String type,
			String origin,
			String message,
			Integer code,
			String pid,
			Long jobInstanceId) {
		synchronized (monitorLogging) {
			if (priority != null) {
				MDC.put("priority", priority);
			}
			if (type != null) {
				MDC.put("type", type);
			}
			if (origin != null) {
				MDC.put("origin", origin);
			}
			if (code != null) {
				MDC.put("code", code);
			}
			MDC.put("actualPid", pid);
			if (jobInstanceId != null) {
				MDC.put("actualJobInstanceId", jobInstanceId);
			}
			switch (priority) {
			case 6:  // fatal
				jobLogger.fatal(message);
				break;
			case 5:  // error
				jobLogger.error(message);
				break;			
			case 4:  // warn
				jobLogger.warn(message);
				break;
			case 3:  // info
				jobLogger.info(message);
				break;
			case 2:  // debug
				jobLogger.debug(message);
				break;
			case 1:  // trace
				jobLogger.trace(message);
				break;
			default:
				jobLogger.debug(message);
			} 
		}
	}

	public long getJobInstanceId() {
		return jobInstanceId;
	}

	public void setJobInstanceId(Long jobInstanceId) {
		if (jobInstanceId != null) {
			this.jobInstanceId = jobInstanceId;
			MDC.put("jobInstanceId", jobInstanceId);
		}
	}

	public long getProcessInstanceId() {
		return processInstanceId;
	}

	public void setProcessInstanceId(Long processInstanceId) {
		if (processInstanceId != null) {
			this.processInstanceId = processInstanceId;
			MDC.put("processInstanceId", processInstanceId);
		}
	}
	
	public void setProject(String project) {
		this.project = project;
		MDC.put("project", project);
	}
	
	public void setJobName(String jobName) {
		this.jobName = jobName;
		MDC.put("jobName", jobName);
	}
	
	public void setContext(String context) {
		this.context = context;
		MDC.put("context", context);
	}

	public void setPid(String pid) {
		this.talendPid = pid;
		MDC.put("talendPid", pid);
	}

	public void setRootPid(String rootPid) {
		this.talendRootPid = rootPid;
		MDC.put("talendRootPid", rootPid);
	}

	public void setFatherPid(String fatherPid) {
		this.talendFatherPid = fatherPid;
		MDC.put("talendFatherPid", fatherPid);
	}

	public void setVersion(String version) {
		MDC.put("version", version);
	}
	
	public void addContextVar(String key, Object value) {
		if (value != null) {
			MDC.put("context." + key, value);
		}
	}

	public static void setRootLoggerName(String rootLoggerName) {
		if (rootLoggerName != null && rootLoggerName.trim().isEmpty() == false) {
			Log4JHelper.rootLoggerName = rootLoggerName;
		}
	}

	public String getLoggerNamePattern() {
		return loggerNamePattern;
	}

	public void setLoggerNamePattern(String loggerNamePattern) {
		if (loggerNamePattern != null && loggerNamePattern.trim().isEmpty() == false) {
			this.loggerNamePattern = loggerNamePattern.trim();
		}
	}
	
	private static Object monitorConfig = new Object();
	
	public void catchStandardOut(boolean forwardToConsole) {
		synchronized(monitorConfig) {
			if (stdOutRedirected == false) {
				System.out.println("Job: " + this.jobName + " [" + this.talendPid + "] redirects System out and err.");
				stdOutRedirected = true;
				System.setOut(new LoggerPrintStream(new LoggerOutputStreamStd(System.out, forwardToConsole))); // Info
				System.setErr(new LoggerPrintStream(new LoggerOutputStreamErr(System.err, forwardToConsole))); // Error
			}
		}
	}
	
	public void addAppender(Appender appender) {
		if (appender != null) {
			if (talendRoot.getAppender(appender.getName()) == null) {
				talendRoot.addAppender(appender);
			}
		}
	}
	
	public void close() {
		if (fileAppender != null) {
			fileAppender.close();
			talendRoot.removeAppender(fileAppender);
		}
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public static void setLog4jConfigFile(String log4jConfigFile) {
		Log4JHelper.log4jConfigFile = log4jConfigFile;
	}

	public long getJobStartedAt() {
		return jobStartedAt;
	}

	public void setJobStartedAt(long jobStartedAt) {
		this.jobStartedAt = jobStartedAt;
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(jobStartedAt);
		MDC.put(KEY_START_TS_COMPACT, sdfTsCompact.format(cal.getTime()));
		MDC.put(KEY_START_TS_LONG, sdfTsLong.format(cal.getTime()));
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		MDC.put(KEY_START_DATE_COMPACT, sdfDayCompact.format(cal.getTime()));
		MDC.put(KEY_START_DATE_LONG, sdfDayLong.format(cal.getTime()));
	}

	public String getPid() {
		return talendPid;
	}

	public String getWorkItem() {
		return workItem;
	}

	public void setWorkItem(String workItem) {
		if (workItem != null && workItem.trim().isEmpty() == false) {
			this.workItem = workItem;
			MDC.put(KEY_WORK_ITEM, workItem);
		}
	}

}
