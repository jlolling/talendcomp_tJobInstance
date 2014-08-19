package de.cimt.talendcomp.manage;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import de.cimt.talendcomp.log4j.JobInstanceLogDBAppender;
import de.cimt.talendcomp.process.ProcessHelper;

public class JobInstanceHelper {
	
	public static final String TABLE_JOB_INSTANCE_STATUS = "JOB_INSTANCE_STATUS";
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
	private Connection connection = null;
	private String tableName = TABLE_JOB_INSTANCE_STATUS;
	private String contextTableName = null;
	private String logTableName = null;
	private String schemaName = null;
	private String sequenceExpression = null;
	private boolean autoIncrementColumn = true;
	private String processInfo = null;
	private JobInfo prevJobInfo;
	private JobInfo currentJobInfo;
	private JobInstanceCounterHelper ch = new JobInstanceCounterHelper();
	private String okResultCodes = null;
	private boolean hasPrevInstance = false;
	private JobInstanceLogDBAppender logDbAppender = null;
	private int messageMaxLength = 1000;
	private Map<String, String> alternativeColumnNames = new HashMap<String, String>();
	private static Map<String, Properties> bundleCache = new HashMap<String, Properties>();
	private String logLayoutPattern = null;
	private Integer logBatchPeriodMillis = null;
	private Integer logBatchSize = null;
	
	public JobInstanceHelper() {
		currentJobInfo = new JobInfo();
		retrieveProcessInfo();
	}
	
	public Appender getAppender() {
		if (logDbAppender == null) {
			logDbAppender = new JobInstanceLogDBAppender(connection, currentJobInfo.getJobInstanceId());
			logDbAppender.setTableName(logTableName);
			logDbAppender.setSchemaName(schemaName);
			logDbAppender.setAlternativeColumnNames(alternativeColumnNames);
			logDbAppender.setBatchPeriodMillis(logBatchPeriodMillis);
			if (logLayoutPattern != null) {
				logDbAppender.setLayout(new PatternLayout(logLayoutPattern));
			}
		}
		return logDbAppender;
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
	
	public long createEntry() throws SQLException {
		long id = 0;
		if (currentJobInfo.isRootJob() == false && currentJobInfo.getProcessInstanceId() == 0) {
			id = selectJobInstanceId(connection, currentJobInfo.getRootJobGuid());
			if (id > 0) {
				currentJobInfo.setProcessInstanceId(id);
			} else if (currentJobInfo.getParentJobGuid() != null && currentJobInfo.getParentJobGuid().isEmpty() == false) {
				id = selectJobInstanceId(connection, currentJobInfo.getParentJobGuid());
				if (id > 0) {
					currentJobInfo.setProcessInstanceId(id);
				}
			}
		}
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ");
		sb.append(getTable());
		sb.append(" (");
		if (autoIncrementColumn == false) {
			sb.append(getColumn(JOB_INSTANCE_ID)); // 1
			sb.append(",");
		}
		sb.append(getColumn(JOB_NAME)); // 2
		sb.append(",");
		sb.append(getColumn(JOB_GUID)); // 3
		sb.append(",");
		sb.append(getColumn(ROOT_JOB_GUID)); // 4
		sb.append(",");
		sb.append(getColumn(JOB_WORK_ITEM)); // 5
		sb.append(",");
		sb.append(getColumn(JOB_TIME_RANGE_START)); // 6
		sb.append(",");
		sb.append(getColumn(JOB_TIME_RANGE_END)); // 7
		sb.append(",");
		sb.append(getColumn(JOB_VALUE_RANGE_START)); // 8
		sb.append(",");
		sb.append(getColumn(JOB_VALUE_RANGE_END)); // 9
		sb.append(",");
		sb.append(getColumn(JOB_STARTED_AT)); // 10
		sb.append(",");
		sb.append(getColumn(PROCESS_INSTANCE_ID)); // 11
		sb.append(",");
		sb.append(PROCESS_INSTANCE_NAME); // 12
		sb.append(",");
		sb.append(getColumn(JOB_DISPLAY_NAME)); // 13
		sb.append(",");
		sb.append(getColumn(JOB_HOST_NAME)); // 14
		sb.append(",");
		sb.append(getColumn(JOB_HOST_PID)); // 15
		sb.append(",");
		sb.append(getColumn(JOB_EXT_ID)); // 16
		sb.append(",");
		sb.append(getColumn(JOB_INFO)); // 17
		sb.append(",");
		sb.append(getColumn(JOB_HOST_USER)); // 18
		sb.append(",");
		sb.append(getColumn(JOB_PROJECT)); // 18
		sb.append(")");
		sb.append(" values (");
		if (autoIncrementColumn == false) {
			sb.append("("); // first field job_instance_id
			sb.append(sequenceExpression);
			sb.append("),");
		}
		// parameter 1-17
		sb.append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		boolean wasAutoCommit = connection.getAutoCommit();
		if (wasAutoCommit == false) {
			connection.setAutoCommit(true);
		}
		String insertSQL = sb.toString();
		PreparedStatement psInsert = connection.prepareStatement(insertSQL);
		psInsert.setString(1, currentJobInfo.getName());
		psInsert.setString(2, currentJobInfo.getGuid());
		if (currentJobInfo.getRootJobGuid() != null) {
			psInsert.setString(3, currentJobInfo.getRootJobGuid());
		} else {
			psInsert.setNull(3, Types.VARCHAR);
		}
		psInsert.setString(4, currentJobInfo.getWorkItem());
		if (currentJobInfo.getTimeRangeStart() != null) {
			psInsert.setTimestamp(5, new Timestamp(currentJobInfo.getTimeRangeStart().getTime()));
		} else {
			psInsert.setNull(5, Types.TIMESTAMP);
		}
		if (currentJobInfo.getTimeRangeEnd() != null) {
			psInsert.setTimestamp(6, new Timestamp(currentJobInfo.getTimeRangeEnd().getTime()));
		} else {
			psInsert.setNull(6, Types.TIMESTAMP);
		}
		if (currentJobInfo.getValueRangeStart() != null) {
			psInsert.setString(7, currentJobInfo.getValueRangeStart());
		} else {
			psInsert.setNull(7, Types.VARCHAR);
		}
		if (currentJobInfo.getValueRangeEnd() != null) {
			psInsert.setString(8, currentJobInfo.getValueRangeEnd());
		} else {
			psInsert.setNull(8, Types.VARCHAR);
		}
		psInsert.setTimestamp(9, new Timestamp(currentJobInfo.getStartDate().getTime()));
		psInsert.setLong(10, currentJobInfo.getProcessInstanceId());
		psInsert.setString(11, currentJobInfo.getProcessInstanceName());
		psInsert.setString(12, currentJobInfo.getJobDisplayName());
		psInsert.setString(13, currentJobInfo.getHostName());
		psInsert.setInt(14, currentJobInfo.getHostPid());
		psInsert.setString(15, currentJobInfo.getExtJobId());
		psInsert.setString(16, currentJobInfo.getJobInfo());
		psInsert.setString(17, currentJobInfo.getHostUser());
		psInsert.setString(18, currentJobInfo.getProject());
		int count = psInsert.executeUpdate();
		psInsert.close();
		if (count == 0) {
			throw new SQLException("No dataset inserted!");
		}
		currentJobInfo.setJobInstanceId(selectJobInstanceId(connection, currentJobInfo.getGuid()));
		if (wasAutoCommit == false) {
			connection.setAutoCommit(false);
		}
		if (currentJobInfo.getJobInstanceId() == -1) {
			throw new SQLException("No job_instances entry found for jobGuid=" + currentJobInfo.getGuid());
		}
		return currentJobInfo.getJobInstanceId();
	}
	
	public int cleanupByWorkItem() throws SQLException {
		if (currentJobInfo.getWorkItem() != null && currentJobInfo.getReturnCode() == 0) {
			StringBuilder sb = new StringBuilder();
			sb.append("delete from ");
			sb.append(getTable());
			sb.append(" where ");
			sb.append(getColumn(JOB_NAME));
			sb.append(" = ?/*#1*/ and ");
			sb.append(getColumn(JOB_WORK_ITEM));
			sb.append(" = ?/*#2*/ and ");
			sb.append(getColumn(JOB_WORK_ITEM));
			sb.append(" is not null and ");
			sb.append(getColumn(JOB_INSTANCE_ID));
			sb.append(" < ?/*#3*/ and (");
			sb.append(getColumn(JOB_RETURN_CODE));
			sb.append(" = 0 or "); // delete only successfully finished or...
			sb.append(getColumn(JOB_ENDED_AT));
			sb.append(" is null)");  // .... aborted runs
			PreparedStatement ps = connection.prepareStatement(sb.toString());
			ps.setString(1, currentJobInfo.getName());
			ps.setString(2, currentJobInfo.getWorkItem());
			ps.setLong(3, currentJobInfo.getJobInstanceId());
			int countDeletedJobInstances = ps.executeUpdate();
			if (connection.getAutoCommit() == false) {
				connection.commit();
			}
			return countDeletedJobInstances;
		} else {
			return 0;
		}
	}
	
	private long selectJobInstanceId(Connection conn, String jobGuid) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("select ");
		sb.append(getColumn(JOB_INSTANCE_ID));
		sb.append(" from ");
		sb.append(getTable());
		sb.append(" where ");
		sb.append(getColumn(JOB_GUID));
		sb.append("=? order by ");
		sb.append(getColumn(JOB_INSTANCE_ID));
		sb.append(" desc");
		PreparedStatement psSelect = connection.prepareStatement(sb.toString());
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
	
	public void updateEntryForStartExistingJob() throws Exception {
		checkConnection(connection);
		StringBuilder sb = new StringBuilder();
		sb.append("update ");
		sb.append(getTable());
		sb.append(" set ");
		sb.append(getColumn(JOB_STARTED_AT)); // 1
		sb.append(" = ?,");
		sb.append(getColumn(JOB_RETURN_CODE));
		sb.append(" = 0 ");
		sb.append("where ");
		sb.append(getColumn(JOB_INSTANCE_ID)); // 2
		sb.append(" = ?");
		boolean wasAutoCommit = connection.getAutoCommit();
		if (wasAutoCommit == false) {
			connection.setAutoCommit(true);
		}
		String updateSQL = sb.toString();
		PreparedStatement psUpdate = connection.prepareStatement(updateSQL);
		currentJobInfo.setStartDate(new Date());
		psUpdate.setTimestamp(1, new Timestamp(currentJobInfo.getStartDate().getTime()));
		psUpdate.setLong(2, currentJobInfo.getJobInstanceId());
		int count = psUpdate.executeUpdate();
		psUpdate.close();
		ch.setJobInstanceId(currentJobInfo.getJobInstanceId());
		if (wasAutoCommit == false) {
			connection.setAutoCommit(false);
		}
		if (count == 0) {
			throw new SQLException("No job instance entry updated for " + getColumn(JOB_INSTANCE_ID) + "=" + currentJobInfo.getJobInstanceId());
		}
	}
	
	public void updateEntry() throws Exception {
		checkConnection(connection);
		StringBuilder sb = new StringBuilder();
		sb.append("update ");
		sb.append(getTable());
		sb.append(" set ");
		sb.append(getColumn(JOB_ENDED_AT)); // 1
		sb.append("=?,");
		sb.append(getColumn(JOB_RESULT_ITEM)); // 2
		sb.append("=?,");
		sb.append(getColumn(JOB_TIME_RANGE_START)); // 3
		sb.append("=?,");
		sb.append(getColumn(JOB_TIME_RANGE_END)); // 4
		sb.append("=?,");
		sb.append(getColumn(JOB_INPUT)); // 5
		sb.append("=?,");
		sb.append(getColumn(JOB_OUTPUT)); // 6
		sb.append("=?,");
		sb.append(getColumn(JOB_REJECTED)); // 7
		sb.append("=?,");
		sb.append(getColumn(JOB_DELETED)); // 8
		sb.append("=?,");
		sb.append(getColumn(JOB_RETURN_CODE)); // 9
		sb.append("=?,");
		sb.append(PROCESS_INSTANCE_NAME); // 10
		sb.append("=?,");
		sb.append(getColumn(JOB_RETURN_MESSAGE)); // 11
		sb.append("=?,");
		sb.append(getColumn(JOB_VALUE_RANGE_START)); // 12
		sb.append("=?,");
		sb.append(getColumn(JOB_VALUE_RANGE_END)); // 13
		sb.append("=?,");
		sb.append(getColumn(JOB_UPDATED)); // 14
		sb.append("=? ");
		sb.append("where ");
		sb.append(getColumn(JOB_INSTANCE_ID)); // 15
		sb.append("=?");
		String updateSQL = sb.toString();
		PreparedStatement psUpdate = connection.prepareStatement(updateSQL);
		psUpdate.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
		if (currentJobInfo.getJobResult() != null) {
			psUpdate.setString(2, currentJobInfo.getJobResult());
		} else {
			psUpdate.setNull(2, Types.VARCHAR);
		}
		if (currentJobInfo.getTimeRangeStart() != null) {
			psUpdate.setTimestamp(3, new Timestamp(currentJobInfo.getTimeRangeStart().getTime()));
		} else {
			psUpdate.setNull(3, Types.TIMESTAMP);
		}
		if (currentJobInfo.getTimeRangeEnd() != null) {
			psUpdate.setTimestamp(4, new Timestamp(currentJobInfo.getTimeRangeEnd().getTime()));
		} else {
			psUpdate.setNull(4, Types.TIMESTAMP);
		}
		psUpdate.setInt(5, currentJobInfo.getCountInput());
		psUpdate.setInt(6, currentJobInfo.getCountOutput());
		psUpdate.setInt(7, currentJobInfo.getCountReject());
		psUpdate.setInt(8, currentJobInfo.getCountDelete());
		psUpdate.setInt(9, currentJobInfo.getReturnCode());
		psUpdate.setString(10, currentJobInfo.getProcessInstanceName());
		psUpdate.setString(11, limitMessage(currentJobInfo.getReturnMessage(), messageMaxLength, 1));
		psUpdate.setString(12, currentJobInfo.getValueRangeStart());
		psUpdate.setString(13, currentJobInfo.getValueRangeEnd());
		psUpdate.setInt(14, currentJobInfo.getCountUpdate());
		psUpdate.setLong(15, currentJobInfo.getJobInstanceId());
		int count = psUpdate.executeUpdate();
		psUpdate.close();
		ch.setJobInstanceId(currentJobInfo.getJobInstanceId());
		ch.writeCounters();
		if (count == 0) {
			throw new SQLException("No dataset updated for job_instance_id=" + currentJobInfo.getJobInstanceId());
		}
	}

	public boolean retrievePreviousInstanceData(boolean successful, boolean withOutput) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("select * from ");
		sb.append(getTable());
		sb.append(" where ");
		sb.append(getColumn(JOB_NAME));
		sb.append("=? and ");
		sb.append(getColumn(JOB_STARTED_AT));
		sb.append(" < ?");
		if (successful) {
			sb.append(" and ");
			sb.append(getColumn(JOB_RETURN_CODE));
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
			sb.append(getColumn(JOB_OUTPUT));
			sb.append(" > 0 or ");
			sb.append(getColumn(JOB_UPDATED));
			sb.append(" > 0 or ");
			sb.append(getColumn(JOB_DELETED));
			sb.append(" > 0)");
		}
		sb.append(" order by ");
		sb.append(getColumn(JOB_STARTED_AT));
		sb.append(" desc");
		PreparedStatement psSelect = connection.prepareStatement(sb.toString());
		psSelect.setString(1, currentJobInfo.getName());
		psSelect.setTimestamp(2, new Timestamp(currentJobInfo.getStartDate().getTime()));
		ResultSet rs = psSelect.executeQuery();
		if (rs.next()) {
			hasPrevInstance = true;
			prevJobInfo = getJobInfoFromResultSet(rs);
		}
		rs.close();
		psSelect.close();
		return hasPrevInstance;
	}
	
	public List<JobInfo> getJobInfosAfter(int jobInstanceId, boolean successful, boolean withOutput, String ... jobNames) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("select * from ");
		sb.append(getTable());
		sb.append(" where ");
		sb.append(getColumn(JOB_INSTANCE_ID));
		sb.append(" > ");
		sb.append(jobInstanceId);
		if (successful) {
			sb.append(" and ");
			sb.append(getColumn(JOB_RETURN_CODE));
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
			sb.append(getColumn(JOB_OUTPUT));
			sb.append(" > 0 or ");
			sb.append(getColumn(JOB_UPDATED));
			sb.append(" > 0 or ");
			sb.append(getColumn(JOB_DELETED));
			sb.append(" > 0)");
		}
		if (jobNames != null && jobNames.length > 0) {
			sb.append(" and ");
			sb.append(getColumn(JOB_NAME));
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
		sb.append(getColumn(JOB_INSTANCE_ID));
		PreparedStatement psSelect = connection.prepareStatement(sb.toString());
		ResultSet rs = psSelect.executeQuery();
		List<JobInfo> list = new ArrayList<JobInfo>();
		while (rs.next()) {
			list.add(getJobInfoFromResultSet(rs));
		}
		rs.close();
		psSelect.close();
		return list;
	}
	
	public boolean isInitialRun() {
		return hasPrevInstance == false;
	}

	public String getJobInstanceIdListAfterPreviousJob(boolean successful, boolean withOutput, String ... jobNames) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("select ");
		sb.append(getColumn(JOB_INSTANCE_ID));
		sb.append(" from ");
		sb.append(getTable());
		sb.append(" where ");
		sb.append(getColumn(JOB_NAME));
		sb.append(" <> '");
		sb.append(currentJobInfo.getName());
		sb.append("'");
		if (hasPrevInstance) {
			sb.append(" and ");
			sb.append(getColumn(JOB_INSTANCE_ID));
			sb.append(" > ");
			sb.append(getPrevJobInstanceId());
		} 
		if (successful) {
			sb.append(" and ");
			sb.append(getColumn(JOB_RETURN_CODE));
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
			sb.append(getColumn(JOB_OUTPUT));
			sb.append(" > 0 or ");
			sb.append(getColumn(JOB_UPDATED));
			sb.append(" > 0 or ");
			sb.append(getColumn(JOB_DELETED));
			sb.append(" > 0)");
		}
		if (jobNames != null && jobNames.length > 0) {
			sb.append(" and ");
			sb.append(getColumn(JOB_NAME));
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
		sb.append(getColumn(JOB_INSTANCE_ID));
		PreparedStatement psSelect = connection.prepareStatement(sb.toString());
		ResultSet rs = psSelect.executeQuery();
		StringBuilder res = new StringBuilder();
		boolean firstLoop = true;
		while (rs.next()) {
			if (firstLoop) {
				firstLoop = false;
			} else {
				res.append(",");
			}
			res.append(rs.getInt(1));
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
		ji.setJobInstanceId(rs.getInt(getColumn(JOB_INSTANCE_ID)));
		ji.setName(rs.getString(getColumn(JOB_NAME)));
		ji.setJobInfo(rs.getString(getColumn(JOB_INFO)));
		ji.setGuid(rs.getString(getColumn(JOB_GUID)));
		ji.setStartDate(rs.getTimestamp(getColumn(JOB_STARTED_AT)));
		ji.setStopDate(rs.getTimestamp(getColumn(JOB_ENDED_AT)));
		ji.setWorkItem(rs.getString(getColumn(JOB_WORK_ITEM)));
		ji.setTimeRangeStart(rs.getTimestamp(getColumn(JOB_TIME_RANGE_START)));
		ji.setTimeRangeEnd(rs.getTimestamp(getColumn(JOB_TIME_RANGE_END)));
		ji.setValueRangeStart(rs.getString(getColumn(JOB_VALUE_RANGE_START)));
		ji.setValueRangeEnd(rs.getString(getColumn(JOB_VALUE_RANGE_END)));
		ji.setDisplayName(rs.getString(getColumn(JOB_DISPLAY_NAME)));
		ji.setProcessInstanceId(rs.getInt(getColumn(PROCESS_INSTANCE_ID)));
		ji.setJobResult(rs.getString(getColumn(JOB_RESULT_ITEM)));
		ji.setCountInput(rs.getInt(getColumn(JOB_INPUT)));
		ji.setCountOutput(rs.getInt(getColumn(JOB_OUTPUT)));
		ji.setCountUpdate(rs.getInt(getColumn(JOB_UPDATED)));
		ji.setCountReject(rs.getInt(getColumn(JOB_REJECTED)));
		ji.setCountDelete(rs.getInt(getColumn(JOB_DELETED)));
		ji.setHostName(rs.getString(getColumn(JOB_HOST_NAME)));
		ji.setHostPid(rs.getInt(getColumn(JOB_HOST_PID)));
		ji.setReturnCode(rs.getInt(getColumn(JOB_RETURN_CODE)));
		ji.setReturnMessage(rs.getString(getColumn(JOB_RETURN_MESSAGE)));
		ji.setExtJobId(rs.getString(getColumn(JOB_EXT_ID)));
		ji.setProject(rs.getString(getColumn(JOB_PROJECT)));
		return ji;
	}
	
	private JobInfo getBrokenJobInfoFromResultSet(ResultSet rs) throws SQLException {
		JobInfo ji = new JobInfo();
		ji.setJobInstanceId(rs.getInt(getColumn(JOB_INSTANCE_ID)));
		ji.setName(rs.getString(getColumn(JOB_NAME)));
		ji.setJobInfo(rs.getString(getColumn(JOB_INFO)));
		ji.setGuid(rs.getString(getColumn(JOB_GUID)));
		ji.setStartDate(rs.getTimestamp(getColumn(JOB_STARTED_AT)));
		ji.setWorkItem(rs.getString(getColumn(JOB_WORK_ITEM)));
		ji.setTimeRangeStart(rs.getTimestamp(getColumn(JOB_TIME_RANGE_START)));
		ji.setTimeRangeEnd(rs.getTimestamp(getColumn(JOB_TIME_RANGE_END)));
		ji.setValueRangeStart(rs.getString(getColumn(JOB_VALUE_RANGE_START)));
		ji.setValueRangeEnd(rs.getString(getColumn(JOB_VALUE_RANGE_END)));
		ji.setDisplayName(rs.getString(getColumn(JOB_DISPLAY_NAME)));
		ji.setProcessInstanceId(rs.getInt(getColumn(PROCESS_INSTANCE_ID)));
		ji.setHostName(rs.getString(getColumn(JOB_HOST_NAME)));
		ji.setHostPid(rs.getInt(getColumn(JOB_HOST_PID)));
		ji.setExtJobId(rs.getString(getColumn(JOB_EXT_ID)));
		ji.setProject(rs.getString(getColumn(JOB_PROJECT)));
		return ji;
	}

	private String getTable() {
		return schemaName != null ? schemaName + "." + tableName : tableName;
	}
	
	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) throws Exception {
		if (connection.isClosed()) {
			throw new Exception("Connection is already closed!");
		} else if (connection.isReadOnly()) {
			throw new Exception("Connection is read only, this component needs to modify data!");
		}
		if (connection.getAutoCommit() == false) {
			connection.setAutoCommit(true);
		}
		this.connection = connection;
		ch.setConnection(connection);
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

	public void setJobWorkItem(Object jobWorkItem) {
		if (jobWorkItem instanceof String) {
			currentJobInfo.setWorkItem((String) jobWorkItem);
		} else if (jobWorkItem instanceof Date) {
			SimpleDateFormat date_param_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			currentJobInfo.setWorkItem(date_param_format.format((Date) jobWorkItem));
		} else if (jobWorkItem != null) {
			currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
		}
	}

	public void setJobWorkItem(int jobWorkItem) {
		currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
	}

	public void setJobWorkItem(long jobWorkItem) {
		currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
	}

	public void setJobWorkItem(short jobWorkItem) {
		currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
	}

	public void setJobWorkItem(char jobWorkItem) {
		currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
	}

	public void setJobWorkItem(byte jobWorkItem) {
		currentJobInfo.setWorkItem(String.valueOf(jobWorkItem));
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
			ch.addToCounter(name, in);
		}
	}
	
	public void subCountInput(Number in, String name) {
		currentJobInfo.subCountInput(in);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, in);
		}
	}

	public void addCountOutput(Number out, String name) {
		currentJobInfo.addCountOutput(out);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, out);
		}
	}
	
	public void subCountOutput(Number out, String name) {
		currentJobInfo.subCountOutput(out);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, out);
		}
	}

	public void addCountUpdate(Number out, String name) {
		currentJobInfo.addCountUpdate(out);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, out);
		}
	}

	public void subCountUpdate(Number out, String name) {
		currentJobInfo.subCountUpdate(out);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, out);
		}
	}

	public void addCountReject(Number rej, String name) {
		currentJobInfo.addCountReject(rej);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, rej);
		}
	}
	
	public void subCountReject(Number rej, String name) {
		currentJobInfo.subCountReject(rej);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, rej);
		}
	}

	public void addCountDelete(Number in, String name) {
		if (in != null) {
			currentJobInfo.addCountDelete(in.intValue());
			if (isNotEmpty(name)) {
				ch.addToCounter(name, in.intValue());
			}
		}
	}
	
	public void subCountDelete(Number c, String name) {
		currentJobInfo.subCountDelete(c);
		if (isNotEmpty(name)) {
			ch.addToCounter(name, c);
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
			this.tableName = tableName;
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

	public Integer retrieveJobReturn(Map<String, Object> globalMap, Integer errorCode) {
    	Integer errorCodeFromTDie = errorCode;
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
        	StringBuilder message = new StringBuilder();
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
        		}
        	}
        	if (message.length() > 0) {
        		currentJobInfo.setReturnMessage(message.toString());
        	}
    	}
    	currentJobInfo.setReturnCode(errorCodeFromTDie != null ? errorCodeFromTDie : 0);
    	return errorCodeFromTDie;
    }
    
    public void resetCounter() {
    	currentJobInfo.resetCounter();
    }
    
    public void closeConnection() throws SQLException {
    	if (connection != null) {
    		if (logDbAppender == null) {
    			if (connection.isClosed() == false) {
            		connection.close();
    			}
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
	
	public String getContextTableName() {
		return contextTableName;
	}

	public void setContextTableName(String contextTableName) {
		this.contextTableName = contextTableName;
	}

	public void setCounterTableName(String counterTableName) {
		if (ch != null) {
			ch.setTableName(counterTableName);
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
	public static String limitMessage(String message, int size, int cutPosition) {
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
	
	/**
	 * set alternative names for columns
	 * @param originalName see the constants
	 * @param newName the new name
	 */
	public void setAlternativeColumnName(String originalName, String newName) {
		if (originalName == null || originalName.trim().isEmpty()) {
			throw new IllegalArgumentException("originalName cannot be null or empty");
		}
		if (newName != null && newName.trim().isEmpty() == false) {
			alternativeColumnNames.put(originalName.trim(), newName.trim());
		} else {
			alternativeColumnNames.remove(originalName.trim());
		}
	}
	
	private String getColumn(String originalName) {
		String newName = alternativeColumnNames.get(originalName.toLowerCase());
		if (newName != null) {
			return newName;
		} else {
			return originalName;
		}
	}
	
	public void closeDbAppender() {
		if (logDbAppender != null) {
			logDbAppender.close();
		}
		Logger.getLogger("talend").removeAppender(logDbAppender);
	}
		
	public boolean configure(String bundleName) throws IOException {
		// check if we have already loaded the bundle
		Properties props = bundleCache.get(bundleName);
		if (props == null) {
			// load the bundle from the classpath
			String resourceName = "/mapping_" + bundleName.toLowerCase() + ".properties";
			InputStream in = JobInstanceHelper.class.getClass().getResourceAsStream(resourceName);
			if (in != null) {
				// resource exists
				props = new Properties();
				props.load(in);
				bundleCache.put(bundleName, props);
				in.close();
			}
		}
		if (props != null) {
			// if we have a bundle configure the names and attributes
			for (Map.Entry<Object, Object> entry : props.entrySet()) {
				String key = entry.getKey().toString();
				String value = entry.getValue().toString();
				if (value != null && value.trim().isEmpty() == false) {
					if (key.startsWith("column.")) { // 
						String originalColumnName = key.substring(7);
						alternativeColumnNames.put(originalColumnName, value);
					} else if (key.startsWith("table.")) {
						String originalTableName = key.substring(6);
						if (TABLE_JOB_INSTANCE_STATUS.equalsIgnoreCase(originalTableName)) {
							this.tableName = value;
						} else if (JobInstanceContextHelper.JOB_INSTANCE_CONTEXT.equalsIgnoreCase(originalTableName)) {
							this.contextTableName = value;
						} else if (JobInstanceCounterHelper.JOB_INSTANCE_COUNTERS.equalsIgnoreCase(originalTableName)) {
							ch.setTableName(value);
						} else if (JobInstanceLogDBAppender.JOB_INSTANCE_LOGS.equalsIgnoreCase(originalTableName)) {
							logTableName = value;
						}
					} else if (key.equalsIgnoreCase("maxMessageLength")) {
						try {
							int length = Integer.valueOf(value);
							setMaxMessageLength(length);
						} catch (Exception e) {
							System.err.println("Attrubute " + key + " expects an integer value");
						}
					} else if (key.equalsIgnoreCase("autoIncrement")) {
						setAutoIncrementColumn(Boolean.parseBoolean(value));
					} else if (key.equalsIgnoreCase("sequenceExpression")) {
						setSequenceExpression(value);
					}
				}
			}
			ch.setAlternativeColumnNames(alternativeColumnNames);
			return true;
		} else {
			return false;
		}
	}

	public Map<String, String> getAlternativeColumnNames() {
		return alternativeColumnNames;
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
	
	public List<JobInfo> cleanupBrokenJobInstances() throws Exception {
		checkConnection(connection);
		countRunningJobInstances = 0;
		countBrokenInstances = 0;
		String hostName = currentJobInfo.getHostName();
		final java.sql.Timestamp endedAt = new java.sql.Timestamp(System.currentTimeMillis());
		ProcessHelper ph = new ProcessHelper();
		ph.init();
		final List<Integer> runningPidList = ph.retrieveProcessList();
		countProcesses = runningPidList.size();
		if (countProcesses == 0) {
			throw new Exception("No running OS processes detected, this is not a valid state, abort check!");
		}
		if (lastSystemStart != null) {
			final StringBuilder updateInstanceLastStart = new StringBuilder();
			updateInstanceLastStart.append("update ");
			updateInstanceLastStart.append(getTable());
			updateInstanceLastStart.append(" set ");
			updateInstanceLastStart.append(getColumn(JOB_ENDED_AT));
			updateInstanceLastStart.append("=?, ");
			updateInstanceLastStart.append(getColumn(JOB_INPUT));
			updateInstanceLastStart.append("=0,");
			updateInstanceLastStart.append(getColumn(JOB_OUTPUT));
			updateInstanceLastStart.append("=0,");
			updateInstanceLastStart.append(getColumn(JOB_UPDATED));
			updateInstanceLastStart.append("=0,");
			updateInstanceLastStart.append(getColumn(JOB_REJECTED));
			updateInstanceLastStart.append("=0,");
			updateInstanceLastStart.append(getColumn(JOB_DELETED));
			updateInstanceLastStart.append("=0,");
			updateInstanceLastStart.append(getColumn(JOB_RETURN_CODE));
			updateInstanceLastStart.append("=999,");
			updateInstanceLastStart.append(getColumn(JOB_RETURN_MESSAGE));
			updateInstanceLastStart.append("='Process died' where ");
			updateInstanceLastStart.append(getColumn(JOB_ENDED_AT));
			updateInstanceLastStart.append(" is null and ");
			updateInstanceLastStart.append(getColumn(JOB_STARTED_AT));
			updateInstanceLastStart.append(" <= ?");
			synchronized(connection) {
				PreparedStatement ps = connection.prepareStatement(updateInstanceLastStart.toString());
				ps.setTimestamp(1, new java.sql.Timestamp(lastSystemStart.getTime()));
				ps.setTimestamp(2, new java.sql.Timestamp(lastSystemStart.getTime()));
				countBrokenInstances = ps.executeUpdate();
			}
		}
		final List<JobInfo> runningProcessInstancesList = new ArrayList<JobInfo>();
		StringBuilder select = new StringBuilder();
		select.append("select * from ");
		select.append(getTable());
		select.append(" where ");
		select.append(getColumn(JOB_HOST_NAME));
		select.append("='");
		select.append(hostName);
		select.append("' and ");
		select.append(getColumn(JOB_ENDED_AT));
		select.append(" is null");
		synchronized(connection) {
			Statement stat = connection.createStatement();
			ResultSet rs = stat.executeQuery(select.toString());
			while (rs.next()) {
				runningProcessInstancesList.add(getBrokenJobInfoFromResultSet(rs));
				countRunningJobInstances++;
			}
			rs.close();
			stat.close();
		}
		final List<JobInfo> diedProcessInstances = new ArrayList<JobInfo>();
		for (JobInfo pi : runningProcessInstancesList) {
			if (runningPidList.contains(pi.getHostPid()) == false) {
				diedProcessInstances.add(pi);
			}
		}
		final List<JobInfo> diedJobInstanceIdList = new ArrayList<JobInfo>();
		final StringBuilder updateInstance = new StringBuilder();
		updateInstance.append("update ");
		updateInstance.append(getTable());
		updateInstance.append(" set ");
		updateInstance.append(getColumn(JOB_ENDED_AT));
		updateInstance.append("=?, ");
		updateInstance.append(getColumn(JOB_INPUT));
		updateInstance.append("=0,");
		updateInstance.append(getColumn(JOB_OUTPUT));
		updateInstance.append("=0,");
		updateInstance.append(getColumn(JOB_UPDATED));
		updateInstance.append("=0,");
		updateInstance.append(getColumn(JOB_REJECTED));
		updateInstance.append("=0,");
		updateInstance.append(getColumn(JOB_DELETED));
		updateInstance.append("=0,");
		updateInstance.append(getColumn(JOB_RETURN_CODE));
		updateInstance.append("=999,");
		updateInstance.append(getColumn(JOB_RETURN_MESSAGE));
		updateInstance.append("='Process died' where ");
		updateInstance.append(getColumn(JOB_ENDED_AT));
		updateInstance.append(" is null and ");
		updateInstance.append(getColumn(JOB_INSTANCE_ID));
		updateInstance.append("=?");
		synchronized(connection) {
			PreparedStatement psUpdate = connection.prepareStatement(updateInstance.toString());
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
		checkConnection(conn, 0);
	}
	
	private void checkConnection(Connection conn, int countAttempts) throws Exception {
		String message = null;
		synchronized (conn) {
			Statement stat = null;
			try {
				String testSQL = "select " + getColumn(JOB_INSTANCE_ID) + " from " + getTable() + " where " + getColumn(JOB_INSTANCE_ID) + "=0";
				stat = conn.createStatement();
				// this will fail of there is something wrong with the connection
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
	
}
