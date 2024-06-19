/**
 * Copyright 2024 Jan Lolling jan.lolling@gmail.com
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
package de.cimt.talendcomp.jobinstance.manage;

import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.cimt.talendcomp.jobinstance.jmx.TalendJobInfoMXBean;
import de.cimt.talendcomp.jobinstance.process.ProcessHelper;

public class JobInstanceHelper {
	
	private final static Logger logger = LogManager.getLogger(JobInstanceHelper.class);
	public static final String TABLE_JOB_INSTANCE_STATUS = "JOB_INSTANCE_STATUS";
	public static final String VIEW_JOB_INSTANCE_STATUS = "JOB_INSTANCE_STATUS_VIEW";
	public static final String JOB_INSTANCE_ID = "JOB_INSTANCE_ID";
	public static final String PROCESS_INSTANCE_ID = "PROCESS_INSTANCE_ID";
	public static final String PROCESS_INSTANCE_NAME = "PROCESS_INSTANCE_NAME";
	public static final String JOB_NAME = "JOB_NAME";
	public static final String JOB_PROJECT = "JOB_PROJECT";
	public static final String JOB_DISPLAY_NAME = "JOB_DISPLAY_NAME";
	public static final String JOB_GUID = "JOB_GUID";
	public static final String JOB_EXT_ID = "JOB_EXT_ID";
	public static final String JOB_INFO = "JOB_INFO";
	public static final String ROOT_JOB_GUID = "ROOT_JOB_GUID";
	public static final String JOB_WORK_ITEM = "WORK_ITEM";
	public static final String JOB_TIME_RANGE_START = "TIME_RANGE_START";
	public static final String JOB_TIME_RANGE_END = "TIME_RANGE_END";
	public static final String JOB_VALUE_RANGE_START = "VALUE_RANGE_START";
	public static final String JOB_VALUE_RANGE_END = "VALUE_RANGE_END";
	public static final String JOB_STARTED_AT = "JOB_STARTED_AT";
	public static final String JOB_ENDED_AT = "JOB_ENDED_AT";
	public static final String JOB_RESULT_ITEM = "JOB_RESULT";
	public static final String JOB_INPUT = "COUNT_INPUT";
	public static final String JOB_OUTPUT = "COUNT_OUTPUT";
	public static final String JOB_UPDATED = "COUNT_UPDATED";
	public static final String JOB_REJECTED = "COUNT_REJECTED";
	public static final String JOB_DELETED = "COUNT_DELETED";
	public static final String JOB_RETURN_CODE = "RETURN_CODE";
	public static final String JOB_RETURN_MESSAGE = "RETURN_MESSAGE";
	public static final String JOB_HOST_NAME = "HOST_NAME";
	public static final String JOB_HOST_PID = "HOST_PID";
	public static final String JOB_HOST_USER = "HOST_USER";
	public static final String OVERRIDE_DIE_CODE_KEY = "OVERRIDE_DIE_CODE";
	private Connection startConnection = null;
	private Connection endConnection = null;
	private String tableNameStatus = TABLE_JOB_INSTANCE_STATUS;
	private String viewName = VIEW_JOB_INSTANCE_STATUS;
	private String schemaName = null;
	private String sequenceExpression = null;
	private boolean autoIncrementColumn = true;
	private String processInfo = null;
	private JobInfo prevJobInfo;
	private JobInfo currentJobInfo;
	private JobInstanceCounterHelper ch = new JobInstanceCounterHelper();
	private String okResultCodes = null;
	private boolean hasPrevInstance = false;
	private int messageMaxLength = 1000;
	private String logLayoutPattern = null;
	private Integer logBatchPeriodMillis = null;
	private Integer logBatchSize = null;
	private static long maxUsedMemory = 0;
	private static long maximumReachedAt = 0;
	private static long maxTotalMemory = 0;
	private static long maxMemory = 0;
	private static Thread memoryMonitor = null;
	protected static boolean debug = false;
	private int returnCodeForDeadInstances = 999;
	private Map<String, Integer> scannerCounterMap = new HashMap<String, Integer>();
	private boolean useViewToReadStatus = false;
	private boolean useGeneratedJID = false;
	private static JID jid = new JID();
	private int delayForCheckInstancesInSec = 0;
	
	public JobInstanceHelper() {
		currentJobInfo = new JobInfo();
		retrieveProcessInfo();
	}
	
	private void retrieveProcessInfo() {
		processInfo = ManagementFactory.getRuntimeMXBean().getName();
		int p = processInfo.indexOf('@');
		if (p > 0) {
			currentJobInfo.setHostPid(Integer.valueOf(processInfo.substring(0, p)));
			currentJobInfo.setHostName(processInfo.substring(p + 1));
		} else {
			currentJobInfo.setHostName(processInfo);
		}
		currentJobInfo.setHostUser(System.getProperty("user.name"));
	}
	
	public long createEntry() throws Exception {
		checkConnection(startConnection);
		if (currentJobInfo.isRootJob() == false && currentJobInfo.getProcessInstanceId() == 0) {
			long id = selectJobInstanceId(startConnection, currentJobInfo.getRootJobGuid());
			if (id > 0) {
				currentJobInfo.setProcessInstanceId(id);
			} else if (currentJobInfo.getParentJobGuid() != null && currentJobInfo.getParentJobGuid().isEmpty() == false) {
				id = selectJobInstanceId(startConnection, currentJobInfo.getParentJobGuid());
				if (id > 0) {
					currentJobInfo.setProcessInstanceId(id);
				}
			}
		}
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ");
		sb.append(getTable(true));
		sb.append(" (");
		if (autoIncrementColumn == false || useGeneratedJID) {
			sb.append(JOB_INSTANCE_ID); // 1
			sb.append(",");
		}
		sb.append(JOB_NAME); // 2
		sb.append(",");
		sb.append(JOB_GUID); // 3
		sb.append(",");
		sb.append(ROOT_JOB_GUID); // 4
		sb.append(",");
		sb.append(JOB_WORK_ITEM); // 5
		sb.append(",");
		sb.append(JOB_TIME_RANGE_START); // 6
		sb.append(",");
		sb.append(JOB_TIME_RANGE_END); // 7
		sb.append(",");
		sb.append(JOB_VALUE_RANGE_START); // 8
		sb.append(",");
		sb.append(JOB_VALUE_RANGE_END); // 9
		sb.append(",");
		sb.append(JOB_STARTED_AT); // 10
		sb.append(",");
		sb.append(PROCESS_INSTANCE_ID); // 11
		sb.append(",");
		sb.append(JOB_HOST_NAME); // 12
		sb.append(",");
		sb.append(JOB_HOST_PID); // 13
		sb.append(",");
		sb.append(JOB_EXT_ID); // 14
		sb.append(",");
		sb.append(JOB_INFO); // 15
		sb.append(",");
		sb.append(JOB_HOST_USER); // 16
		sb.append(",");
		sb.append(JOB_PROJECT); // 17
		sb.append(",");
		sb.append(PROCESS_INSTANCE_NAME); // 18
		sb.append(",");
		sb.append(JOB_DISPLAY_NAME); // 19
		sb.append(")");
		sb.append(" values (");
		int paramIndex = 1;
		if (useGeneratedJID) {
			sb.append("?,");
		} else {
			if (autoIncrementColumn == false) {
				sb.append("("); // first field job_instance_id
				sb.append(sequenceExpression);
				sb.append("),");
			}
		}
		// parameter 1-18 or 2-19
		sb.append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		String sql = sb.toString();
		debug(sql);
		PreparedStatement psInsert = null;
		try {
			psInsert = startConnection.prepareStatement(sql,
					(autoIncrementColumn ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS));
		} catch (Exception nse) {
			// Snowflake and Exasol database does not support the return of generated keys
			psInsert = startConnection.prepareStatement(sql);
			autoIncrementColumn = false;
		}
		// #1
		// start set parameters
		if (useGeneratedJID) {
			long genJid = jid.createJID();
			if (isDebug()) {
				debug("Use generated job_instance_id=" + genJid);
			}
			psInsert.setLong(paramIndex++, genJid);
		}
		psInsert.setString(paramIndex++, currentJobInfo.getName());
		if (currentJobInfo.getGuid() == null) {
			throw new IllegalStateException("Job guid is null. Please call setJobGuid(String) before!");
		}
		psInsert.setString(paramIndex++, currentJobInfo.getGuid());
		if (currentJobInfo.getRootJobGuid() != null) {
			psInsert.setString(paramIndex++, currentJobInfo.getRootJobGuid());
		} else {
			psInsert.setNull(paramIndex++, Types.VARCHAR);
		}
		if (currentJobInfo.getWorkItem() != null) {
			psInsert.setString(paramIndex++, currentJobInfo.getWorkItem());
		} else {
			psInsert.setNull(paramIndex++, Types.VARCHAR);
		}
		if (currentJobInfo.getTimeRangeStart() != null) {
			psInsert.setTimestamp(paramIndex++, new Timestamp(currentJobInfo.getTimeRangeStart().getTime()));
		} else {
			psInsert.setNull(paramIndex++, Types.TIMESTAMP);
		}
		if (currentJobInfo.getTimeRangeEnd() != null) {
			psInsert.setTimestamp(paramIndex++, new Timestamp(currentJobInfo.getTimeRangeEnd().getTime()));
		} else {
			psInsert.setNull(paramIndex++, Types.TIMESTAMP);
		}
		if (currentJobInfo.getValueRangeStart() != null) {
			psInsert.setString(paramIndex++, currentJobInfo.getValueRangeStart());
		} else {
			psInsert.setNull(paramIndex++, Types.VARCHAR);
		}
		if (currentJobInfo.getValueRangeEnd() != null) {
			psInsert.setString(paramIndex++, currentJobInfo.getValueRangeEnd());
		} else {
			psInsert.setNull(paramIndex++, Types.VARCHAR);
		}
		if (currentJobInfo.getStartDate() == null) {
			throw new IllegalArgumentException("Job start date is null. Please call setJobStartedAt(long) before!");
		}
		psInsert.setTimestamp(paramIndex++, new Timestamp(currentJobInfo.getStartDate().getTime()));
		psInsert.setLong(paramIndex++, currentJobInfo.getProcessInstanceId());
		psInsert.setString(paramIndex++, currentJobInfo.getHostName());
		psInsert.setInt(paramIndex++, currentJobInfo.getHostPid());
		psInsert.setString(paramIndex++, currentJobInfo.getExtJobId());
		psInsert.setString(paramIndex++, currentJobInfo.getJobInfo());
		psInsert.setString(paramIndex++, currentJobInfo.getHostUser());
		psInsert.setString(paramIndex++, currentJobInfo.getProject());
		if (currentJobInfo.getProcessInstanceName() != null) {
			psInsert.setString(paramIndex++, currentJobInfo.getProcessInstanceName());
		} else {
			psInsert.setNull(paramIndex++, Types.VARCHAR);
		}
		if (currentJobInfo.getJobDisplayName() != null) {
			psInsert.setString(paramIndex++, currentJobInfo.getJobDisplayName());
		} else {
			psInsert.setNull(paramIndex++, Types.VARCHAR);
		}
		int count = psInsert.executeUpdate();
		if (count == 0) {
			throw new SQLException("No dataset inserted!");
		}
		long currentJobInstanceId = -1;
		if (useGeneratedJID) {
			psInsert.close();
			currentJobInstanceId = jid.getJID();
		} else if (autoIncrementColumn) {
			// sometimes this does not work
			ResultSet rsKeys = psInsert.getGeneratedKeys();
			if (rsKeys.next()) {
				currentJobInstanceId = rsKeys.getLong(1);
			}
			rsKeys.close();
			psInsert.close();
		} else {
			psInsert.close();
			currentJobInstanceId = selectJobInstanceId(startConnection, currentJobInfo.getGuid());
		}
		currentJobInfo.setJobInstanceId(currentJobInstanceId);
		if (currentJobInfo.getJobInstanceId() == -1) {
			throw new SQLException("No job_instances entry found for jobGuid=" + currentJobInfo.getGuid());
		}
		return currentJobInfo.getJobInstanceId();
	}
	
	public int cleanupByWorkItem() throws SQLException {
		if (currentJobInfo.getWorkItem() != null && currentJobInfo.getReturnCode() == 0) {
			StringBuilder sb = new StringBuilder();
			sb.append("delete from ");
			sb.append(getTable(true));
			sb.append(" where ");
			sb.append(JOB_NAME);
			sb.append(" = ?/*#1*/ and ");
			sb.append(JOB_WORK_ITEM);
			sb.append(" = ?/*#2*/ and ");
			sb.append(JOB_WORK_ITEM);
			sb.append(" is not null and ");
			sb.append(JOB_INSTANCE_ID);
			sb.append(" < ?/*#3*/ and ");
			sb.append(JOB_RETURN_CODE);
			if (okResultCodes != null) {
				sb.append(" in (");
				sb.append(okResultCodes);
				sb.append(") ");
			} else {
				sb.append(" = 0 ");
			}
			sb.append(" and ");
			sb.append(JOB_ENDED_AT);
			sb.append(" is not null");  // do not delete running entries
			String sql = sb.toString();
			debug(sql);
			PreparedStatement ps = endConnection.prepareStatement(sql);
			ps.setString(1, currentJobInfo.getName());
			ps.setString(2, currentJobInfo.getWorkItem());
			ps.setLong(3, currentJobInfo.getJobInstanceId());
			int countDeletedJobInstances = ps.executeUpdate();
			if (endConnection.getAutoCommit() == false) {
				endConnection.commit();
			}
			return countDeletedJobInstances;
		} else {
			return 0;
		}
	}
	
	public int cleanupByTimerange(int hoursToKeep) throws SQLException {
		if (currentJobInfo.getWorkItem() != null && currentJobInfo.getReturnCode() == 0) {
			StringBuilder sb = new StringBuilder();
			sb.append("delete from ");
			sb.append(getTable(true));
			sb.append(" where ");
			sb.append(JOB_NAME);
			sb.append(" = ?/*#1*/ and ");
			sb.append(JOB_INSTANCE_ID);
			sb.append(" < ?/*#2*/ and ");
			sb.append(JOB_RETURN_CODE);
			if (okResultCodes != null) {
				sb.append(" in (");
				sb.append(okResultCodes);
				sb.append(") ");
			} else {
				sb.append(" = 0 ");
			}
			sb.append(" and ");
			sb.append(JOB_ENDED_AT);
			sb.append(" is not null");  // do not delete running entries
			sb.append(" and ");
			sb.append(JOB_STARTED_AT);
			sb.append(" < ?/*#3*/");
			// get the Date pointing to the past
			Date dateUntilReadyToDelete = new Date(currentJobInfo.getStartDate().getTime() - (hoursToKeep * 60l * 60l * 1000l));
			debug("Remove job instances older than " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateUntilReadyToDelete));
			String sql = sb.toString();
			debug(sql);
			PreparedStatement ps = endConnection.prepareStatement(sql);
			ps.setString(1, currentJobInfo.getName());
			ps.setLong(2, currentJobInfo.getJobInstanceId());
			ps.setTimestamp(3, new java.sql.Timestamp(dateUntilReadyToDelete.getTime()));
			int countDeletedJobInstances = ps.executeUpdate();
			if (endConnection.getAutoCommit() == false) {
				endConnection.commit();
			}
			return countDeletedJobInstances;
		} else {
			return 0;
		}
	}

	private long selectJobInstanceId(Connection conn, String jobGuid) throws SQLException {
		if (jobGuid == null || jobGuid.trim().isEmpty()) {
			throw new IllegalArgumentException("jobGuid cannot be null or empty");
		}
		StringBuilder sb = new StringBuilder();
		sb.append("select ");
		sb.append(JOB_INSTANCE_ID);
		sb.append(" from ");
		sb.append(getTable(false));
		sb.append(" where ");
		sb.append(JOB_GUID);
		sb.append("=? order by ");
		sb.append(JOB_INSTANCE_ID);
		sb.append(" desc");
		String sql = sb.toString();
		debug(sql);
		PreparedStatement psSelect = startConnection.prepareStatement(sql);
		psSelect.setString(1, jobGuid);
		ResultSet rs = psSelect.executeQuery();
		long id = 0;
		if (rs.next()) {
			id = rs.getLong(1);
		}
		rs.close();
		psSelect.close();
		return id;
	}

	public void updateEntry() throws Exception {
		checkConnection(endConnection);
		StringBuilder sb = new StringBuilder();
		sb.append("update ");
		sb.append(getTable(false));
		sb.append(" set ");
		sb.append(JOB_ENDED_AT); // 1
		sb.append("=?,");
		sb.append(JOB_RESULT_ITEM); // 2
		sb.append("=?,");
		sb.append(JOB_TIME_RANGE_START); // 3
		sb.append("=?,");
		sb.append(JOB_TIME_RANGE_END); // 4
		sb.append("=?,");
		sb.append(JOB_INPUT); // 5
		sb.append("=?,");
		sb.append(JOB_OUTPUT); // 6
		sb.append("=?,");
		sb.append(JOB_REJECTED); // 7
		sb.append("=?,");
		sb.append(JOB_DELETED); // 8
		sb.append("=?,");
		sb.append(JOB_RETURN_CODE); // 9
		sb.append("=?,");
		sb.append(JOB_RETURN_MESSAGE); // 10
		sb.append("=?,");
		sb.append(JOB_VALUE_RANGE_START); // 11
		sb.append("=?,");
		sb.append(JOB_VALUE_RANGE_END); // 12
		sb.append("=?,");
		sb.append(JOB_UPDATED); // 13
		sb.append("=? ");
		sb.append("where ");
		sb.append(JOB_INSTANCE_ID); // 14
		sb.append("=?");
		String sql = sb.toString();
		debug(sql);
		PreparedStatement psUpdate = endConnection.prepareStatement(sql);
		int paramIndex = 1;
		psUpdate.setTimestamp(paramIndex++, new Timestamp(System.currentTimeMillis()));
		if (currentJobInfo.getJobResult() != null) {
			psUpdate.setString(paramIndex++, currentJobInfo.getJobResult());
		} else {
			psUpdate.setNull(paramIndex++, Types.VARCHAR);
		}
		if (currentJobInfo.getTimeRangeStart() != null) {
			psUpdate.setTimestamp(paramIndex++, new Timestamp(currentJobInfo.getTimeRangeStart().getTime()));
		} else {
			psUpdate.setNull(paramIndex++, Types.TIMESTAMP);
		}
		if (currentJobInfo.getTimeRangeEnd() != null) {
			psUpdate.setTimestamp(paramIndex++, new Timestamp(currentJobInfo.getTimeRangeEnd().getTime()));
		} else {
			psUpdate.setNull(paramIndex++, Types.TIMESTAMP);
		}
		psUpdate.setInt(paramIndex++, currentJobInfo.getCountInput());
		psUpdate.setInt(paramIndex++, currentJobInfo.getCountOutput());
		psUpdate.setInt(paramIndex++, currentJobInfo.getCountReject());
		psUpdate.setInt(paramIndex++, currentJobInfo.getCountDelete());
		psUpdate.setInt(paramIndex++, currentJobInfo.getReturnCode());
		psUpdate.setString(paramIndex++, enforceTextLength(currentJobInfo.getReturnMessage(), messageMaxLength, 1));
		if (currentJobInfo.getValueRangeStart() != null) {
			psUpdate.setString(paramIndex++, currentJobInfo.getValueRangeStart());
		} else {
			psUpdate.setNull(paramIndex++, Types.VARCHAR);
		}
		if (currentJobInfo.getValueRangeEnd() != null) {
			psUpdate.setString(paramIndex++, currentJobInfo.getValueRangeEnd());
		} else {
			psUpdate.setNull(paramIndex++, Types.VARCHAR);
		}
		psUpdate.setInt(paramIndex++, currentJobInfo.getCountUpdate());
		psUpdate.setLong(paramIndex++, currentJobInfo.getJobInstanceId());
		int count = psUpdate.executeUpdate();
		psUpdate.close();
		ch.setJobInstanceId(currentJobInfo.getJobInstanceId());
		ch.writeCounters();
		if (count == 0) {
			throw new SQLException("No dataset updated for job_instance_id=" + currentJobInfo.getJobInstanceId());
		}
	}

	public boolean retrievePreviousInstanceData(boolean successful, boolean withOutput, boolean withInput, boolean forWorkItem, boolean sameRoot) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("select * from ");
		sb.append(getTable(false));
		sb.append(" where ");
		sb.append(JOB_NAME);
		sb.append("= ? and ");
		sb.append(JOB_INSTANCE_ID);
		sb.append(" < ?");		
		boolean searchForWorkItem = false;
		if (forWorkItem) {
			sb.append(" and ");
			sb.append(JOB_WORK_ITEM);
			if (currentJobInfo.getWorkItem() != null) {
				sb.append(" = ?");
				searchForWorkItem = true;
			} else {
				sb.append(" is null ");
			}
		}
		if (successful) {
			sb.append(" and ");
			sb.append(JOB_RETURN_CODE);
			if (okResultCodes != null) {
				sb.append(" in (");
				sb.append(okResultCodes);
				sb.append(") ");
			} else {
				sb.append(" = 0 ");
			}
		}
		if (withOutput) {
			sb.append(" and (");
			sb.append(JOB_OUTPUT);
			sb.append(" > 0 or ");
			sb.append(JOB_UPDATED);
			sb.append(" > 0 or ");
			sb.append(JOB_DELETED);
			sb.append(" > 0)");
		}
		if (withInput) {
			sb.append(" and (");
			sb.append(JOB_INPUT);
			sb.append(" > 0 )");
		}
		if (sameRoot) {
			sb.append(" and ");
			sb.append(PROCESS_INSTANCE_ID);
			sb.append(" = ");
			sb.append(currentJobInfo.getProcessInstanceId());
		}
		sb.append(" order by ");
		sb.append(JOB_INSTANCE_ID);
		sb.append(" desc");
		String sql = sb.toString();
		debug(sql);
		PreparedStatement psSelect = startConnection.prepareStatement(sql);
		psSelect.setString(1, currentJobInfo.getName());
		psSelect.setLong(2, currentJobInfo.getJobInstanceId());
		if (searchForWorkItem) {
			psSelect.setString(3, currentJobInfo.getWorkItem());
		}
		ResultSet rs = psSelect.executeQuery();
		if (rs.next()) {
			hasPrevInstance = true;
			prevJobInfo = getJobInfoFromResultSet(rs);
		}
		rs.close();
		psSelect.close();
		return hasPrevInstance;
	}
	
	public JobInfo getJobInstanceAlreadyRunning(boolean forWorkItem) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("select * from ");
		sb.append(getTable(false));
		sb.append(" where ");
		sb.append(JOB_NAME);
		sb.append(" = ? ");
		boolean searchForWorkItem = false;
		if (forWorkItem) {
			sb.append(" and ");
			sb.append(JOB_WORK_ITEM);
			if (currentJobInfo.getWorkItem() != null) {
				sb.append(" = ?");
				searchForWorkItem = true;
			} else {
				sb.append(" is null ");
			}
		}
		sb.append(" and JOB_ENDED_AT is null");
		sb.append(" order by ");
		sb.append(JOB_INSTANCE_ID);
		sb.append(" desc");
		String sql = sb.toString();
		debug(sql);
		PreparedStatement psSelect = startConnection.prepareStatement(sql);
		psSelect.setString(1, currentJobInfo.getName());
		if (searchForWorkItem) {
			psSelect.setString(2, currentJobInfo.getWorkItem());
		}
		JobInfo jobRunning = null;
		ResultSet rs = psSelect.executeQuery();
		if (rs.next()) {
			jobRunning = getJobInfoFromResultSet(rs); 
		}
		rs.close();
		psSelect.close();
		return jobRunning;
	}

	public boolean isInitialRun() {
		return hasPrevInstance == false;
	}

	public String getJobInstanceIdListAfterPreviousJob(boolean successful, boolean withOutput, boolean withInput, String ... jobNames) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("select ");
		sb.append(JOB_INSTANCE_ID);
		sb.append(" from ");
		sb.append(getTable(false));
		sb.append(" where ");
		sb.append(JOB_NAME);
		sb.append(" <> '");
		sb.append(currentJobInfo.getName());
		sb.append("'");
		if (hasPrevInstance) {
			sb.append(" and ");
			sb.append(JOB_INSTANCE_ID);
			sb.append(" > ");
			sb.append(getPrevJobInstanceId());
		} 
		if (successful) {
			sb.append(" and ");
			sb.append(JOB_RETURN_CODE);
			if (okResultCodes != null) {
				sb.append(" in (");
				sb.append(okResultCodes);
				sb.append(") ");
			} else {
				sb.append(" = 0 ");
			}
		}
		if (withOutput) {
			sb.append(" and (");
			sb.append(JOB_OUTPUT);
			sb.append(" > 0 or ");
			sb.append(JOB_UPDATED);
			sb.append(" > 0 or ");
			sb.append(JOB_DELETED);
			sb.append(" > 0)");
		}
		if (withInput) {
			sb.append(" and (");
			sb.append(JOB_INPUT);
			sb.append(" > 0 )");
		}
		if (jobNames != null && jobNames.length > 0) {
			sb.append(" and ");
			sb.append(JOB_NAME);
			sb.append(" in (");
			boolean firstLoop = true;
			for (String jobName : jobNames) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(",");
				}
				sb.append("'");
				sb.append(jobName);
				sb.append("'");
			}
			sb.append(")");
		}
		sb.append(" order by ");
		sb.append(JOB_INSTANCE_ID);
		String sql = sb.toString();
		debug(sql);
		PreparedStatement psSelect = startConnection.prepareStatement(sql);
		ResultSet rs = psSelect.executeQuery();
		StringBuilder res = new StringBuilder();
		boolean firstLoop = true;
		while (rs.next()) {
			if (firstLoop) {
				firstLoop = false;
			} else {
				res.append(",");
			}
			res.append(rs.getLong(1));
		}
		if (firstLoop) {
			// no jobs found, create dummy list
			res.append("0");
		}
		rs.close();
		psSelect.close();
		return res.toString();
	}

	private JobInfo getJobInfoFromResultSet(ResultSet rs) throws SQLException {
		JobInfo ji = new JobInfo();
		ji.setJobInstanceId(rs.getLong(JOB_INSTANCE_ID));
		ji.setName(rs.getString(JOB_NAME));
		ji.setJobInfo(rs.getString(JOB_INFO));
		ji.setGuid(rs.getString(JOB_GUID));
		ji.setStartDate(rs.getTimestamp(JOB_STARTED_AT));
		ji.setStopDate(rs.getTimestamp(JOB_ENDED_AT));
		ji.setWorkItem(rs.getString(JOB_WORK_ITEM));
		ji.setTimeRangeStart(rs.getTimestamp(JOB_TIME_RANGE_START));
		ji.setTimeRangeEnd(rs.getTimestamp(JOB_TIME_RANGE_END));
		ji.setValueRangeStart(rs.getString(JOB_VALUE_RANGE_START));
		ji.setValueRangeEnd(rs.getString(JOB_VALUE_RANGE_END));
		ji.setProcessInstanceId(rs.getLong(PROCESS_INSTANCE_ID));
		ji.setDisplayName(rs.getString(JOB_DISPLAY_NAME));
		ji.setProcessInstanceId(rs.getLong(PROCESS_INSTANCE_ID));		
		ji.setJobResult(rs.getString(JOB_RESULT_ITEM));
		ji.setCountInput(rs.getInt(JOB_INPUT));
		ji.setCountOutput(rs.getInt(JOB_OUTPUT));
		ji.setCountUpdate(rs.getInt(JOB_UPDATED));
		ji.setCountReject(rs.getInt(JOB_REJECTED));
		ji.setCountDelete(rs.getInt(JOB_DELETED));
		ji.setHostName(rs.getString(JOB_HOST_NAME));
		ji.setHostPid(rs.getInt(JOB_HOST_PID));
		ji.setReturnCode(rs.getInt(JOB_RETURN_CODE));
		ji.setReturnMessage(rs.getString(JOB_RETURN_MESSAGE));
		ji.setExtJobId(rs.getString(JOB_EXT_ID));
		ji.setProject(rs.getString(JOB_PROJECT));
		return ji;
	}
	
	private JobInfo getBrokenJobInfoFromResultSet(ResultSet rs) throws SQLException {
		JobInfo ji = new JobInfo();
		ji.setJobInstanceId(rs.getLong(JOB_INSTANCE_ID));
		ji.setName(rs.getString(JOB_NAME));
		ji.setJobInfo(rs.getString(JOB_INFO));
		ji.setGuid(rs.getString(JOB_GUID));
		ji.setStartDate(rs.getTimestamp(JOB_STARTED_AT));
		ji.setWorkItem(rs.getString(JOB_WORK_ITEM));
		ji.setTimeRangeStart(rs.getTimestamp(JOB_TIME_RANGE_START));
		ji.setTimeRangeEnd(rs.getTimestamp(JOB_TIME_RANGE_END));
		ji.setValueRangeStart(rs.getString(JOB_VALUE_RANGE_START));
		ji.setValueRangeEnd(rs.getString(JOB_VALUE_RANGE_END));
		ji.setProcessInstanceId(rs.getLong(PROCESS_INSTANCE_ID));
		ji.setHostName(rs.getString(JOB_HOST_NAME));
		ji.setHostPid(rs.getInt(JOB_HOST_PID));
		ji.setExtJobId(rs.getString(JOB_EXT_ID));
		ji.setProject(rs.getString(JOB_PROJECT));
		return ji;
	}

	private String getTable(boolean write) {
		if (write) {
			return schemaName != null ? schemaName + "." + tableNameStatus : tableNameStatus;
		} else {
			if (useViewToReadStatus) {
				return schemaName != null ? schemaName + "." + viewName : viewName;
			} else {
				return schemaName != null ? schemaName + "." + tableNameStatus : tableNameStatus;
			}
		}
	}
	
	public Connection getConnection() {
		return startConnection;
	}
	
	public Connection getEndConnection() {
		return endConnection;
	}

	public void setConnection(Connection connection) throws Exception {
		if (connection == null) {
			throw new IllegalArgumentException("No connection given. In case of using a data source check the alias.");
		}
		if (connection.isClosed()) {
			throw new Exception("Connection is already closed!");
		} else if (connection.isReadOnly()) {
			throw new Exception("Connection is read only, this component needs to modify data!");
		}
		if (connection.getAutoCommit() == false) {
			connection.setAutoCommit(true);
		}
		this.startConnection = connection;
		this.endConnection = connection;
		ch.setConnection(this.startConnection);
	}
	
	public void setEndConnection(Connection connection) throws Exception {
		if (connection == null) {
			throw new IllegalArgumentException("No end connection given. In case of using a data source check the alias.");
		}
		if (connection.isClosed()) {
			throw new Exception("Connection is already closed!");
		} else if (connection.isReadOnly()) {
			throw new Exception("Connection is read only, this component needs to modify data!");
		}
		if (connection.getAutoCommit() == false) {
			connection.setAutoCommit(true);
		}
		if (startConnection == connection) {
			System.err.println("As connection for tJobInstanceEnd should be used a different connection than for tJobInstanceStart!");
		}
		this.endConnection = connection;
		ch.setConnection(this.endConnection);
	}

	public long getJobInstanceId() {
		return currentJobInfo.getJobInstanceId();
	}

	public String getJobName() {
		return currentJobInfo.getName();
	}

	public void setJobName(String jobName) {
		currentJobInfo.setName(jobName);
	}

	public String getJobGuid() {
		return currentJobInfo.getGuid();
	}

	public void setJobGuid(String jobGuid) {
		this.currentJobInfo.setGuid(jobGuid != null ? jobGuid.trim() : null);
	}

	public String getExtJobId() {
		return currentJobInfo.getExtJobId();
	}

	public void setExtJobId(String extJobId) {
		this.currentJobInfo.setExtJobId(extJobId != null ? extJobId.trim() : null);
	}

	public String getJobInfo() {
		return currentJobInfo.getJobInfo();
	}

	public void setJobInfo(String jobInfo) {
		this.currentJobInfo.setJobInfo(jobInfo != null ? jobInfo.trim() : null);
	}

	public String getJobWorkItem() {
		return currentJobInfo.getWorkItem();
	}

	public void setJobWorkItem(Object jobWorkItem, boolean emptyAsNull) {
		if (jobWorkItem instanceof String) {
			String value = (String) jobWorkItem;
			if (value.isEmpty() == false || emptyAsNull == false) {
				currentJobInfo.setWorkItem(value);
			}
		} else if (jobWorkItem instanceof Date) {
			SimpleDateFormat date_param_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			currentJobInfo.setWorkItem(date_param_format.format((Date) jobWorkItem));
		} else if (jobWorkItem != null) {
			String value = String.valueOf(jobWorkItem);
			if (value.isEmpty() == false || emptyAsNull == false) {
				currentJobInfo.setWorkItem(value);
			}
		}
	}
	
	public static String getCleanedContext(String contextStr) {
		return contextStr.trim().replaceAll("_[0-9]{4,}$", "");
	}

	public void setJobWorkItem(Integer jobWorkItem, boolean emptyAsNull) {
		if (jobWorkItem != null) {
			currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
		}
	}

	public void setJobWorkItem(int jobWorkItem, boolean emptyAsNull) {
		currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
	}

	public void setJobWorkItem(Long jobWorkItem, boolean emptyAsNull) {
		if (jobWorkItem != null) {
			currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
		}
	}

	public void setJobWorkItem(long jobWorkItem, boolean emptyAsNull) {
		currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
	}

	public void setJobWorkItem(Short jobWorkItem, boolean emptyAsNull) {
		if (jobWorkItem != null) {
			currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
		}
	}

	public void setJobWorkItem(Double jobWorkItem, boolean emptyAsNull) {
		if (jobWorkItem != null) {
			currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
		}
	}

	public void setJobWorkItem(BigDecimal jobWorkItem, boolean emptyAsNull) {
		if (jobWorkItem != null) {
			currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
		}
	}

	public void setJobWorkItem(Character jobWorkItem, boolean emptyAsNull) {
		if (jobWorkItem != null) {
			currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
		}
	}

	public void setJobWorkItem(Byte jobWorkItem, boolean emptyAsNull) {
		if (jobWorkItem != null) {
			currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
		}
	}

	public void setJobWorkItem(byte[] jobWorkItem, boolean emptyAsNull) {
		if (jobWorkItem != null) {
        	byte[] byteArray = jobWorkItem;
        	if (byteArray.length > 0) {
            	StringBuilder sb = new StringBuilder(byteArray.length * 2);
            	sb.append("x'");
            	for (byte b : byteArray) {
            		sb.append(String.format("%02X", b));
            	}
            	sb.append("'");
    			currentJobInfo.setWorkItem(String.valueOf(sb.toString()));
        	}
		}
	}
	
	public String getJobResult() {
		return currentJobInfo.getJobResult();
	}

	public void setJobResult(String jobResult) {
		currentJobInfo.setJobResult(jobResult);
	}

	public int getCountInput() {
		return currentJobInfo.getCountInput();
	}

	public void addCountInput(Number in, String name) {
		currentJobInfo.addCountInput(in);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, in, "input");
		}
	}
	
	public void subCountInput(Number in, String name) {
		currentJobInfo.subCountInput(in);
		if (isNotEmpty(name)) {
			ch.subToCounter(name, in, "input");
		}
	}

	public int getCountOutput() {
		return currentJobInfo.getCountOutput();
	}

	public void addCountOutput(Number out, String name) {
		currentJobInfo.addCountOutput(out);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, out, "output");
		}
	}
	
	public void subCountOutput(Number out, String name) {
		currentJobInfo.subCountOutput(out);
		if (isNotEmpty(name)) {
			ch.subToCounter(name, out, "output");
		}
	}

	public void addCountUpdate(Number out, String name) {
		currentJobInfo.addCountUpdate(out);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, out, "update");
		}
	}

	public void subCountUpdate(Number out, String name) {
		currentJobInfo.subCountUpdate(out);
		if (isNotEmpty(name)) {
			ch.subToCounter(name, out, "update");
		}
	}

	public void addCountReject(Number rej, String name) {
		currentJobInfo.addCountReject(rej);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, rej, "reject");
		}
	}
	
	public void subCountReject(Number rej, String name) {
		currentJobInfo.subCountReject(rej);
		if (isNotEmpty(name)) {
			ch.subToCounter(name, rej, "reject");
		}
	}

	public void addCountDelete(Number in, String name) {
		if (in != null) {
			currentJobInfo.addCountDelete(in.intValue());
			if (isNotEmpty(name)) {
				ch.addToCounter(name, in.intValue(), "delete");
			}
		}
	}
	
	public void subCountDelete(Number c, String name) {
		currentJobInfo.subCountDelete(c);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, c, "delete");
		}
	}

	public int getCountRejected() {
		return currentJobInfo.getCountReject();
	}

	public int getReturnCode() {
		return currentJobInfo.getReturnCode();
	}

	public void setReturnCode(Integer returnCode) {
		if (returnCode != null) {
			currentJobInfo.setReturnCode(returnCode);
		}
	}

	public String getReturnMessage() {
		return currentJobInfo.getReturnMessage();
	}

	public void setReturnMessage(String returnMessage) {
		currentJobInfo.setReturnMessage(returnMessage);
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		if (schemaName != null && schemaName.trim().isEmpty() == false) {
			this.schemaName = schemaName;
			ch.setSchemaName(schemaName);
		}
	}

	public String getSequenceExpression() {
		return sequenceExpression;
	}

	public void setSequenceExpression(String sequenceExpression) {
		if (sequenceExpression != null && sequenceExpression.trim().isEmpty() == false) {
			if (sequenceExpression.indexOf("__SCHEMA_NAME__") != -1) {
				sequenceExpression = sequenceExpression.replace("__SCHEMA_NAME__", this.schemaName);
			}
			this.sequenceExpression = sequenceExpression;
		}
	}

	public boolean isAutoIncrementColumn() {
		return autoIncrementColumn;
	}

	public void setAutoIncrementColumn(boolean autoIncrementColumn) {
		this.autoIncrementColumn = autoIncrementColumn;
	}

	private boolean isNotEmpty(String s) {
		return (s != null && s.trim().isEmpty() == false);
	}
	
	public void setTableName(String tableName) {
		if (tableName != null && tableName.trim().isEmpty() == false) {
			this.tableNameStatus = tableName;
		}
	}
	
	public Date getStartDate() {
		return currentJobInfo.getStartDate();
	}

	public Date getTimeRangeStart() {
		return currentJobInfo.getTimeRangeStart();
	}

	public void setTimeRangeStart(Date timeRangeStart) {
		if (timeRangeStart != null) {
			currentJobInfo.setTimeRangeStart(timeRangeStart);
		}
	}

	public void setTimeRangeStart(Long timeRangeStart) {
		if (timeRangeStart != null) {
			currentJobInfo.setTimeRangeStart(new Date(timeRangeStart));
		}
	}

	public void checkTimeRange(Date timeRangeDate) {
		currentJobInfo.checkTimeRange(timeRangeDate);
	}
	
	public void checkValueRange(Long newValue) {
		currentJobInfo.checkValueRange(newValue);
	}

	public void checkValueRange(Short newValue) {
		currentJobInfo.checkValueRange(newValue);
	}

	public void checkValueRange(Double newValue) {
		currentJobInfo.checkValueRange(newValue);
	}

	public void checkValueRange(Float newValue) {
		currentJobInfo.checkValueRange(newValue);
	}

	public void checkValueRange(Byte newValue) {
		currentJobInfo.checkValueRange(newValue);
	}

	public void checkValueRange(Integer newValue) {
		currentJobInfo.checkValueRange(newValue);
	}

	public void checkValueRange(String newValue) {
		currentJobInfo.checkValueRange(newValue);
	}

	public void checkValueRange(Character newValue) {
		currentJobInfo.checkValueRange(newValue);
	}

	public void checkValueRange(BigDecimal newValue) {
		currentJobInfo.checkValueRange(newValue);
	}

	public Date getTimeRangeEnd() {
		return currentJobInfo.getTimeRangeEnd();
	}

	public void setTimeRangeEnd(Date timeRangeEnd) {
		if (timeRangeEnd != null) {
			currentJobInfo.setTimeRangeEnd(timeRangeEnd);
		}
	}

	public void setTimeRangeEnd(Long timeRangeEnd) {
		if (timeRangeEnd != null) {
			currentJobInfo.setTimeRangeEnd(new Date(timeRangeEnd));
		}
	}

	public static JobExit retrieveJobReturn(Map<String, Object> globalMap, Integer errorCode) {
    	Integer errorCodeFromTDie = errorCode;
    	StringBuilder message = new StringBuilder();
    	if (globalMap.containsKey(OVERRIDE_DIE_CODE_KEY)) {
    		Integer code = (Integer) globalMap.get(OVERRIDE_DIE_CODE_KEY);
			if (code.intValue() != 0) {
				errorCodeFromTDie = code;
			}
    	} else {
    		String tDieMessage = null;
    		String compName = null;
        	for (Map.Entry<String, Object> entry : globalMap.entrySet()) {
        		String key = entry.getKey();
    			compName = key.replace("_DIE_CODE", "");
        		if (key.endsWith("_DIE_CODE")) {
        			if (entry.getValue() instanceof Integer) {
        				Integer code = (Integer) entry.getValue();
        				if (code.intValue() != 0) {
        					errorCodeFromTDie = code;
        					key = key.replace("_DIE_CODE", "_DIE_MESSAGE");
        					tDieMessage = (String) globalMap.get(key);
        					break;
        				}
        			}
        		}
        	}
        	if (tDieMessage != null && tDieMessage.trim().isEmpty() == false && "the end is near".equalsIgnoreCase(tDieMessage) == false) {
    			message.append(compName);
    			message.append(":");
        		message.append(tDieMessage);
        		message.append("\n");
        	}
    		// get error message
        	for (Map.Entry<String, Object> entry : globalMap.entrySet()) {
        		String key = entry.getKey();
        		if (key.endsWith("_ERROR_MESSAGE") && entry.getValue() !=  null) {
        			compName = key.replace("_ERROR_MESSAGE", "");
        			message.append(compName);
        			message.append(":");
            		message.append(entry.getValue().toString());
            		message.append("\n");
            		if (compName.contains("tRunJob")) {
            			message.append(compName + " returncode: " + globalMap.get(compName + "_CHILD_RETURN_CODE"));
            			message.append("\n" + compName + " stacktrace: " + globalMap.get(compName + "_CHILD_EXCEPTION_STACKTRACE"));
            		}
        		}
        	}
    	}
		JobExit exit = new JobExit(message.toString(), errorCodeFromTDie);
    	return exit;
    }
    
    public void resetCounter() {
    	currentJobInfo.resetCounter();
    }
    
    public void closeConnection() throws SQLException {
    	if (startConnection != null) {
			if (startConnection.isClosed() == false) {
        		startConnection.close();
			}
    	}
    	if (endConnection != null) {
    		if (endConnection != startConnection) {
    			endConnection.close();
    		}
    	}
    }

	public long getProcessInstanceId() {
		return currentJobInfo.getProcessInstanceId();
	}

	public void setProcessInstanceId(int processInstanceId) {
		currentJobInfo.setProcessInstanceId(processInstanceId);
	}

	public String getProcessInstanceName() {
		return currentJobInfo.getProcessInstanceName();
	}

	public void setProcessInstanceName(String processInstanceName) {
		if (processInstanceName != null && processInstanceName.trim().isEmpty() == false) {
			currentJobInfo.setProcessInstanceName(processInstanceName.trim());
		}
	}

	public void setProject(String projectName) {
		if (projectName != null && projectName.trim().isEmpty() == false) {
			currentJobInfo.setProject(projectName.trim());
		}
	}

	public String getRootJobGuid() {
		return currentJobInfo.getRootJobGuid();
	}

	public String getParentJobGuid() {
		return currentJobInfo.getParentJobGuid();
	}

	public void setRootJobGuid(String rootJobGuid) {
		currentJobInfo.setRootJobGuid(rootJobGuid != null ? rootJobGuid.trim() : null);
	}

	public void setParentJobGuid(String parentJobGuid) {
		currentJobInfo.setParentJobGuid(parentJobGuid != null ? parentJobGuid.trim() : null);
	}

	public String getJobDisplayName() {
		return currentJobInfo.getDisplayName();
	}

	public void setJobDisplayName(String jobDisplayName) {
		currentJobInfo.setDisplayName(jobDisplayName != null ? jobDisplayName.trim() : null);
	}
	
	public int getOSPid() {
		return currentJobInfo.getHostPid();
	}
	
	public String getHostName() {
		return currentJobInfo.getHostName();
	}

	public int getCountDeleted() {
		return currentJobInfo.getCountDelete();
	}

	public long getPrevJobInstanceId() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getJobInstanceId();
	}

	public String getPrevWorkItem() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getWorkItem();
	}

	public Date getPrevTimeRangeStart() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getTimeRangeStart();
	}

	public Date getPrevTimeRangeEnd() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getTimeRangeEnd();
	}

	public String getPrevValueRangeStart() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getValueRangeStart();
	}

	public String getPrevValueRangeEnd() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getValueRangeEnd();
	}

	public int getPrevInput() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getCountInput();
	}

	public int getPrevOutput() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getCountOutput();
	}

	public int getPrevUpdated() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getCountUpdate();
	}

	public int getPrevRejects() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getCountReject();
	}

	public int getPrevDeleted() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getCountDelete();
	}

	public String getPrevJobResult() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getJobResult();
	}

	public String getPrevJobGuid() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getGuid();
	}

	public int getPrevHostPid() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getHostPid();
	}

	public String getPrevHostName() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getHostName();
	}

	public Date getPrevJobStartDate() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getStartDate();
	}

	public Date getPrevJobStopDate() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getStopDate();
	}

	public int getPrevReturnCode() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getReturnCode();
	}

	public String getPrevReturnMessage() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getReturnMessage();
	}

	public String getPrevJobDisplayName() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getJobDisplayName();
	}

	public String getPrevProcessInstanceName() {
		if (prevJobInfo == null) {
			throw new IllegalStateException("prev job info not retrieved!");
		}
		return prevJobInfo.getProcessInstanceName();
	}

	public String getValueRangeStart() {
		return currentJobInfo.getValueRangeStart();
	}

	public void setValueRangeStart(Object valueRangeStart) {
		if (valueRangeStart != null) {
			String value = String.valueOf(valueRangeStart).trim();
			if (value.isEmpty() == false) {
				currentJobInfo.setValueRangeStart(value);
			}
		}
	}

	public String getValueRangeEnd() {
		return currentJobInfo.getValueRangeEnd();
	}

	public void setValueRangeEnd(Object valueRangeEnd) {
		if (valueRangeEnd != null) {
			String value = String.valueOf(valueRangeEnd).trim();
			if (value.isEmpty() == false) {
				currentJobInfo.setValueRangeEnd(value);
			}
		}
	}
	
	public String getOkResultCodes() {
		return okResultCodes;
	}

	public void setOkResultCodes(String okResultCodes) {
		if (okResultCodes != null && okResultCodes.trim().isEmpty() == false) {
			this.okResultCodes = okResultCodes.trim();
		} else {
			this.okResultCodes = null;
		}
	}
	
	/**
	 * limits the message text to avoid overflow database field
	 * @param size to limit
	 * @param cutPosition 0= cuts at end, 1= cuts in the middle, 2=cuts at the start
	 * @return limited text
	 */
	public static String enforceTextLength(String message, int size, int cutPosition) {
		if (message != null && message.trim().isEmpty() == false) {
			message = message.trim();
			if (message.length() > size) {
				size = size - 3; // to have space for "..."
				if (cutPosition == 0) {
					return message.substring(0, size) + "...";
				} else if (cutPosition == 1) {
					StringBuilder sb = new StringBuilder();
					sb.append(message.substring(0, size / 2));
					sb.append("...");
					sb.append(message.substring(message.length() - size / 2, message.length()));
					return sb.toString();
				} else {
					return "..." + message.substring(message.length() - size);
				}
			} else {
				return message;
			}
		} else {
			return null;
		}
	}

	public void setMaxMessageLength(int messageMaxLength) {
		this.messageMaxLength = messageMaxLength;
	}
	
	public String getLogLayoutPattern() {
		return logLayoutPattern;
	}

	public void setLogLayoutPattern(String logLayoutPattern) {
		this.logLayoutPattern = logLayoutPattern;
	}

	public Integer getLogBatchPeriodMillis() {
		return logBatchPeriodMillis;
	}

	public void setLogBatchPeriodMillis(Integer batchPeriodMillis) {
		this.logBatchPeriodMillis = batchPeriodMillis;
	}
	
	public Date getJobStartedAt() {
		return currentJobInfo.getStartDate();
	}

	public void setJobStartedAt(long jobStartedAt) {
		currentJobInfo.setStartDate(new Date(jobStartedAt));
	}

	public Integer getLogBatchSize() {
		return logBatchSize;
	}

	public void setLogBatchSize(Integer logBatchSize) {
		this.logBatchSize = logBatchSize;
	}
	
	public void setProcessInstanceId(Long processInstanceId) {
		if (processInstanceId != null && processInstanceId.longValue() > 0l) {
			currentJobInfo.setProcessInstanceId(processInstanceId);
		}
	}
	
	private int countProcesses = 0;
	private int countRunningJobInstances = 0;
	private int countBrokenInstances = 0;
	private Date lastSystemStart = null;
	
	private String unixCommand = "ps -eo pid=";
	private String unixPidPattern = "([0-9]{1,8})";
	private String windowsCommand = "tasklist /fo list";
	private String windowsPidPattern = "PID[:\\s]*([0-9]{1,6})";
	
	public void setUnixCommand(String unixCommand) {
		if (unixCommand != null && unixCommand.trim().isEmpty() == false) {
			this.unixCommand = unixCommand;
		}
	}

	public String getUnixPidPattern() {
		return unixPidPattern;
	}

	public void setUnixPidPattern(String unixPidPattern) {
		if (unixPidPattern != null && unixPidPattern.trim().isEmpty() == false) {
			this.unixPidPattern = unixPidPattern;
		}
	}

	public void setWindowsCommand(String windowsCommand) {
		if (windowsCommand != null && windowsCommand.trim().isEmpty() == false) {
			this.windowsCommand = windowsCommand;
		}
	}

	public void setWindowsPidPattern(String windowsPidPattern) {
		if (windowsPidPattern != null && windowsPidPattern.trim().isEmpty() == false) {
			this.windowsPidPattern = windowsPidPattern;
		}
	}

	public List<JobInfo> cleanupBrokenJobInstances() throws Exception {
		checkConnection(startConnection);
		countRunningJobInstances = 0;
		countBrokenInstances = 0;
		String hostName = currentJobInfo.getHostName();
		final java.sql.Timestamp endedAt = new java.sql.Timestamp(System.currentTimeMillis());
		ProcessHelper ph = new ProcessHelper();
		ph.setUnixCommand(unixCommand);
		ph.setWindowsCommand(windowsCommand);
		ph.setUnixPidPattern(unixPidPattern);
		ph.setWindowsPidPattern(windowsPidPattern);
		ph.init();
		if (lastSystemStart != null) {
			final StringBuilder updateInstanceLastStart = new StringBuilder();
			updateInstanceLastStart.append("update ");
			updateInstanceLastStart.append(getTable(true));
			updateInstanceLastStart.append(" set ");
			updateInstanceLastStart.append(JOB_ENDED_AT);
			updateInstanceLastStart.append("=?, ");
			updateInstanceLastStart.append(JOB_INPUT);
			updateInstanceLastStart.append("=0,");
			updateInstanceLastStart.append(JOB_OUTPUT);
			updateInstanceLastStart.append("=0,");
			updateInstanceLastStart.append(JOB_UPDATED);
			updateInstanceLastStart.append("=0,");
			updateInstanceLastStart.append(JOB_REJECTED);
			updateInstanceLastStart.append("=0,");
			updateInstanceLastStart.append(JOB_DELETED);
			updateInstanceLastStart.append("=0,");
			updateInstanceLastStart.append(JOB_RETURN_CODE);
			updateInstanceLastStart.append("=" + returnCodeForDeadInstances + ",");
			updateInstanceLastStart.append(JOB_RETURN_MESSAGE);
			updateInstanceLastStart.append("='Process died' where ");
			updateInstanceLastStart.append(JOB_ENDED_AT);
			updateInstanceLastStart.append(" is null and ");
			updateInstanceLastStart.append(JOB_STARTED_AT);
			updateInstanceLastStart.append(" <= ?");
			synchronized(startConnection) {
				String sql = updateInstanceLastStart.toString();
				debug(sql);
				PreparedStatement ps = startConnection.prepareStatement(sql);
				ps.setTimestamp(1, new java.sql.Timestamp(lastSystemStart.getTime()));
				ps.setTimestamp(2, new java.sql.Timestamp(lastSystemStart.getTime()));
				countBrokenInstances = ps.executeUpdate();
			}
		}
		final List<JobInfo> runningProcessInstancesList = new ArrayList<JobInfo>();
		StringBuilder select = new StringBuilder();
		select.append("select * from ");
		select.append(getTable(false));
		select.append(" where ");
		select.append(JOB_HOST_NAME);
		select.append("='");
		select.append(hostName);
		select.append("' and ");
		select.append(JOB_ENDED_AT);
		select.append(" is null");
		select.append("  and ");
		select.append(JOB_STARTED_AT);
		select.append(" < ?");
		
		synchronized(startConnection) {
			String sql = select.toString();
			debug(sql);
			PreparedStatement stat = startConnection.prepareStatement(sql);
			stat.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis() - (delayForCheckInstancesInSec * 1000l)));
			ResultSet rs = stat.executeQuery();
			while (rs.next()) {
				runningProcessInstancesList.add(getBrokenJobInfoFromResultSet(rs));
				countRunningJobInstances++;
			}
			rs.close();
			stat.close();
		}
		final List<Integer> runningPidList = ph.retrieveProcessList(); // throws an exception if nothing found
		countProcesses = runningPidList.size();
		debug("Found " + countProcesses + " running processes on the server: " + hostName);
		final List<JobInfo> diedProcessInstances = new ArrayList<JobInfo>();
		for (JobInfo pi : runningProcessInstancesList) {
			if (runningPidList.contains(pi.getHostPid()) == false) {
				diedProcessInstances.add(pi);
			}
		}
		debug("Found " + runningProcessInstancesList.size() + " probably running Talend jobs and " + diedProcessInstances.size() + " dead job instances");
		final List<JobInfo> diedJobInstanceIdList = new ArrayList<JobInfo>();
		final StringBuilder updateInstance = new StringBuilder();
		updateInstance.append("update ");
		updateInstance.append(getTable(true));
		updateInstance.append(" set ");
		updateInstance.append(JOB_ENDED_AT);
		updateInstance.append("=?, ");
		updateInstance.append(JOB_INPUT);
		updateInstance.append("=0,");
		updateInstance.append(JOB_OUTPUT);
		updateInstance.append("=0,");
		updateInstance.append(JOB_UPDATED);
		updateInstance.append("=0,");
		updateInstance.append(JOB_REJECTED);
		updateInstance.append("=0,");
		updateInstance.append(JOB_DELETED);
		updateInstance.append("=0,");
		updateInstance.append(JOB_RETURN_CODE);
		updateInstance.append("=" + returnCodeForDeadInstances + ",");
		updateInstance.append(JOB_RETURN_MESSAGE);
		updateInstance.append("='Process died' where ");
		updateInstance.append(JOB_ENDED_AT);
		updateInstance.append(" is null and ");
		updateInstance.append(JOB_INSTANCE_ID);
		updateInstance.append("=?");
		synchronized(startConnection) {
			String sql = updateInstance.toString();
			debug(sql);
			PreparedStatement psUpdate = startConnection.prepareStatement(sql);
			for (JobInfo pi : diedProcessInstances) {
				psUpdate.setTimestamp(1, endedAt);
				psUpdate.setLong(2, pi.getJobInstanceId());
				int count = psUpdate.executeUpdate();
				if (count > 0) {
					diedJobInstanceIdList.add(pi);
					countBrokenInstances++;
				}
			}
			psUpdate.close();
		}
		return diedJobInstanceIdList;
	}

	public int getCountProcesses() {
		return countProcesses;
	}

	public int getCountRunningJobInstances() {
		return countRunningJobInstances;
	}

	public int getCountBrokenInstances() {
		return countBrokenInstances;
	}

	public void setLastSystemStart(String timestampStr) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		setLastSystemStart(sdf.parse(timestampStr));
	}
	
	public void setLastSystemStart(Date lastSystemStart) {
		if (lastSystemStart != null) {
			if (lastSystemStart.before(new Date())) {
				this.lastSystemStart = lastSystemStart;
			} else {
				throw new IllegalArgumentException("Last system start cannot lay in the future: " + lastSystemStart);
			}
		}
	}
	
	private int maxCountCheckAttempts = 3;
	
	private void checkConnection(Connection conn) throws Exception {
		if (conn.getAutoCommit() == false) {
			conn.setAutoCommit(true);
		}
		checkConnection(conn, 0);
	}
	
	private void checkConnection(Connection conn, int countAttempts) throws Exception {
		String message = null;
		synchronized (conn) {
			Statement stat = null;
			try {
				String testSQL = "select " + JOB_INSTANCE_ID + " from " + getTable(false) + " where " + JOB_INSTANCE_ID + "=0";
				stat = conn.createStatement();
				// this will fail if there is something wrong with the connection
				ResultSet rs = stat.executeQuery(testSQL);
				rs.next();
				rs.close();
			} catch (SQLException sqle) {
				message = sqle.getMessage();
			} finally {
				if (stat != null) {
					try {
						stat.close();
					} catch (Exception e) {}
				}
			}
		}
		if (message != null) {
			if (countAttempts < maxCountCheckAttempts) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {}
				checkConnection(conn, countAttempts + 1);
			} else {
				throw new Exception("Check connection failed:" + message);
			}
		}
	}
	
	private static void measureMemoryUsage() {
		Runtime rt = Runtime.getRuntime();
		maxMemory = rt.maxMemory();
		long currentFreeMemory = rt.freeMemory();
		long currentTotalMemory = rt.totalMemory();
		long currentUsedMemory = currentTotalMemory - currentFreeMemory;
		if (maxUsedMemory < currentUsedMemory) {
			maxUsedMemory = currentUsedMemory;
			maximumReachedAt = System.currentTimeMillis();
		}
		if (maxTotalMemory < currentTotalMemory) {
			maxTotalMemory = currentTotalMemory;
		}
	}
	
	public static void startMemoryMonitoring() {
		if (memoryMonitor == null) {
			memoryMonitor = new Thread() {
				@Override
				public void run() {
					measureMemoryUsage();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						return;
					}
				}
			};
			memoryMonitor.start();
		}
	}
	
	public static void stopMemoryMonitoring() {
		if (memoryMonitor != null && memoryMonitor.isAlive()) {
			memoryMonitor.interrupt();
		}
		memoryMonitor = null;
	}

	public static long getMaxUsedMemory() {
		return maxUsedMemory;
	}

	public static long getMaxMemory() {
		return maxMemory;
	}

	public static long getMaxTotalMemory() {
		return maxTotalMemory;
	}
	
	public static double getPercentageUsage() {
		if (maxMemory > 0) {
			return (((double) maxUsedMemory) / ((double) maxMemory)) * 100;
		} else {
			return 0d;
		}
	}
	
	public static void logMemoryUsage() {
		if (maxMemory > 0) {
			// if we have already measured memory, we should update it here at least
			measureMemoryUsage();
			NumberFormat nf = NumberFormat.getInstance();
			nf.setGroupingUsed(true);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault());
			info("Available memory: " + nf.format(maxMemory) + " byte");
			info("Maximum used memory (abs.): " + nf.format(maxUsedMemory) + " byte");
			info("Maximum used memory (rel.): " + nf.format(Math.round(getPercentageUsage())) + " %");
			Calendar c = Calendar.getInstance(TimeZone.getDefault());
			c.setTimeInMillis(maximumReachedAt);
			System.out.println("Maximum of memory usage measured at: " + sdf.format(c.getTime()));
		}
	}

	public int getReturnCodeForDeadInstances() {
		return returnCodeForDeadInstances;
	}

	public void setReturnCodeForDeadInstances(Integer returnCode) {
		if (returnCode != null && returnCode > 0) {
			this.returnCodeForDeadInstances = returnCode.intValue();
		}
	}
	
	public void incrementNbLine(String scannerUniqueName) {
		if (scannerUniqueName == null || scannerUniqueName.trim().isEmpty()) {
			throw new IllegalArgumentException("scanner name cannot be null or empty.");
		}
		synchronized(scannerCounterMap) {
			Integer counter = scannerCounterMap.get(scannerUniqueName + "_nb_line_total");
			if (counter == null) {
				counter = Integer.valueOf(0);
				scannerCounterMap.put(scannerUniqueName + "_nb_line_total", counter);
			}
			scannerCounterMap.put(scannerUniqueName + "_nb_line_total", counter + 1);
			counter = scannerCounterMap.get(scannerUniqueName + "_nb_line");
			if (counter == null) {
				counter = Integer.valueOf(0);
				scannerCounterMap.put(scannerUniqueName + "_nb_line", counter);
			}
			scannerCounterMap.put(scannerUniqueName + "_nb_line", counter + 1);
		}
	}
	
	public int getNbLineTotal(String scannerUniqueName) {
		if (scannerUniqueName == null || scannerUniqueName.trim().isEmpty()) {
			throw new IllegalArgumentException("scanner name cannot be null or empty.");
		}
		Integer counter = scannerCounterMap.get(scannerUniqueName + "_nb_line_total");
		if (counter != null) {
			return counter.intValue();
		} else {
			return 0;
		}
	}
	
	public int getNbLine(String scannerUniqueName) {
		if (scannerUniqueName == null || scannerUniqueName.trim().isEmpty()) {
			throw new IllegalArgumentException("scanner name cannot be null or empty.");
		}
		Integer counter = scannerCounterMap.get(scannerUniqueName + "_nb_line");
		if (counter != null) {
			return counter.intValue();
		} else {
			return 0;
		}
	}

	public void resetNbLine(String scannerUniqueName) {
		if (scannerUniqueName == null || scannerUniqueName.trim().isEmpty()) {
			throw new IllegalArgumentException("scanner name cannot be null or empty.");
		}
		scannerCounterMap.remove(scannerUniqueName + "_nb_line");
	}
	
	public void incrementFlowCount(String scannerUniqueName) {
		if (scannerUniqueName == null || scannerUniqueName.trim().isEmpty()) {
			throw new IllegalArgumentException("scanner name cannot be null or empty.");
		}
		synchronized(scannerCounterMap) {
			Integer counter = scannerCounterMap.get(scannerUniqueName + "_flow");
			if (counter == null) {
				counter = Integer.valueOf(0);
				scannerCounterMap.put(scannerUniqueName + "_flow", counter);
			}
			scannerCounterMap.put(scannerUniqueName + "_flow", counter + 1);
		}
	}
	
	public int getFlowCount(String scannerUniqueName) {
		if (scannerUniqueName == null || scannerUniqueName.trim().isEmpty()) {
			throw new IllegalArgumentException("scanner name cannot be null or empty.");
		}
		Integer counter = scannerCounterMap.get(scannerUniqueName + "_flow");
		if (counter != null) {
			return counter.intValue();
		} else {
			return 0;
		}
	}

	public boolean isUseGeneratedKeys() {
		return useGeneratedJID;
	}

	public void setUseGeneratedKeys(boolean useGeneratedKeys) {
		this.useGeneratedJID = useGeneratedKeys;
	}
	
	public static boolean isDebug() {
		return (logger != null && logger.isDebugEnabled()) || debug;
	}

	public static void debug(String message) {
		if (logger != null) {
			if (logger.isDebugEnabled()) {
				logger.debug(message);
			}
		} else if (isDebug()) {
			System.out.println("DEBUG:" + message);
		}
	}
	
	public static void info(String message) {
		if (logger != null) {
			if (logger.isInfoEnabled()) {
				logger.debug(message);
			}
		} else {
			System.out.println("INFO:" + message);
		}
	}

	public static void error(String message, Throwable t) {
		if (logger != null) {
			if (t != null) {
				logger.error(message, t);
			} else {
				logger.error(message);
			}
		} else {
			System.err.println("ERROR:" + message);
			t.printStackTrace(System.err);
		}
	}
	
	public void setHostIndex(Integer hostIndex) {
		if (hostIndex != null) {
			if (hostIndex > 255 || hostIndex < 0) {
				throw new IllegalArgumentException("Host index must be within 0-255!");
			} else {
				jid.setHostIndex((byte) hostIndex.intValue());
			}
		}
	}
	
	private ObjectName beanName = null;
	
	public void registerTalendJobMBean(TalendJobInfoMXBean mbean) throws Exception {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		beanName = new ObjectName("de.cimt.talendcomp.management:type=" + TalendJobInfoMXBean.class.getSimpleName()+",project=" + currentJobInfo.getProject() + ",job=" + currentJobInfo.getName() + ",job_instance_id=" + currentJobInfo.getJobInstanceId());
		if (mbs.isRegistered(beanName) == false) {
			mbs.registerMBean(mbean, beanName);
		}
	}
	
	public void unregisterTalendJobMBean() {
		if (beanName != null) {
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			try {
				mbs.unregisterMBean(beanName);
			} catch (Exception e) {
				System.err.println("Unable to unregister mbean: " + e.getMessage());
			}
		}
	}

	public void setTableNameCounters(String tableNameCounters) {
		if (tableNameCounters != null && tableNameCounters.trim().isEmpty() == false) {
			ch.setTableName(tableNameCounters);
		}
	}

	public int getDelayForCheckInstancesInSec() {
		return delayForCheckInstancesInSec;
	}

	public void setDelayForCheckInstancesInSec(Integer delayForCheckInstancesInSec) {
		if (delayForCheckInstancesInSec != null) {
			this.delayForCheckInstancesInSec = delayForCheckInstancesInSec;
		}
	}
	
}
