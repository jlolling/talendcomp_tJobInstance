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
package de.cimt.talendcomp.log4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;

import de.cimt.talendcomp.manage.JobInstanceHelper;

public class JobInstanceLogDBAppender extends AppenderSkeleton {

	public static final String JOB_INSTANCE_LOGS = "JOB_INSTANCE_LOGS";
	private static final String LOG_TS = "LOG_TS";
	private static final String LOG_LEVEL = "LOG_LEVEL";
	private static final String LOG_NAME = "LOG_NAME";
	private static final String LOG_MESSAGE = "LOG_MESSAGE";
	private String tableName = JOB_INSTANCE_LOGS;
	private Connection connection;
	private String schemaName = null;
	private String logLevel;
	private int maxMessageLength = 1000;
	private BlockingDeque<LogEntry> dequeue = new LinkedBlockingDeque<LogEntry>(100000);
	private PreparedStatement ps;
	private Thread writerThread;
	private Timer batchTimer = null;
	private boolean closed = false;
	private long jobInstanceId = 0;
	private LogEntry theEnd = new LogEntry();
	private Map<String, String> alternativeColumnNames = new HashMap<String, String>();
	private Layout layout = null;
	private boolean executeNow = true;
	private int batchPeriodMillis = 5000;
	private int maxMessagesUntilUpdate = 100;
	private int messageCountInBatch = 0;
	
	public JobInstanceLogDBAppender(Connection connection, long jobInstanceId) {
		try {
			if (connection == null) {
				throw new IllegalArgumentException("connection cannot be null");
			} else if (connection.isClosed()) {
				throw new IllegalArgumentException("connection cannot be closed");
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		if (jobInstanceId < 1) {
			throw new IllegalArgumentException("jobInstanceId must be greater than 0.");
		}
		this.connection = connection;
		this.jobInstanceId = jobInstanceId;
		setName("talend.jobInstanceLogAppender." + String.valueOf(jobInstanceId));
	}
	
	private final static class LogEntry {
		
		private long logTimestamp;
		private String logMessage;
		private String level;
		private String loggerName;
		
		public String toString() {
			return logTimestamp + ":" + level + ":" + logMessage;
		}
		
	}
	
	public void stopWriterThread() {
		closed = true;
		if (writerThread != null) {
			// send the very last entry
			dequeue.offerLast(theEnd);
			// wait until writer thread has been finished
			while (writerThread.isAlive()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {}
			}
		}
	}
	
	private void checkWriter() {
		if (closed == false && writerThread == null || writerThread.isAlive() == false) {
			writerThread = new Thread(new Runnable() {
				
				@Override
				public void run() {
					while (true) {
						if (Thread.currentThread().isInterrupted()) {
							break;
						}
						LogEntry entry = null;
						try {
							entry = dequeue.takeFirst();
							if (entry == theEnd) {
								flushInserts();
								if (batchTimer != null) {
									batchTimer.cancel();
								}
								break;
							}
							insertLogEntry(entry);
						} catch (InterruptedException e) {
							closed = true;
							break;
						} catch (Exception e) {
							closed = true;
							System.err.println("LogWriter: " + getName() + ": write log entry failed:" + e.getMessage());
							System.err.println("Message:" + entry);
						}
					}
					closeConnection();
				}
				
			});
			writerThread.start();
			if (batchTimer != null) {
				batchTimer.cancel();
			}
			if (batchPeriodMillis > 0) {
				batchTimer = new Timer();
				batchTimer.schedule(new TimerTask() {
					
					@Override
					public void run() {
						executeNow = true;
					}
					
				}, 1, batchPeriodMillis);
			}
		}
	}
	
	/**
	 * logs a message
	 * @param message the message
	 * @param level the level can be TRACE,DEBUG,INFO,WARN,ERROR,FATAL
	 */
	public void log(final String message, String level) {
		if (message != null) {
			final LogEntry e = new LogEntry();
			e.logTimestamp = System.currentTimeMillis();
			if (level == null || level.isEmpty()) {
				throw new IllegalArgumentException("level cannot be empty");
			}
			e.level = level;
			e.logMessage = message;
			if (dequeue.offerLast(e) == false) {
				System.err.println("Message queue is ful, message will not persist");
			}
			checkWriter();
		}
	}

	public Connection getConnection() {
		return connection;
	}
	
	private String getTable() {
		return schemaName != null ? schemaName + "." + tableName : tableName;
	}
	
	private PreparedStatement createPreparedStatement() throws SQLException {
		if (connection == null) {
			throw new IllegalStateException("No connection set");
		}
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ");
		sb.append(getTable());
		sb.append(" (");
		sb.append(getColumn(JobInstanceHelper.JOB_INSTANCE_ID));
		sb.append(",");
		sb.append(LOG_TS);
		sb.append(",");
		sb.append(LOG_LEVEL);
		sb.append(",");
		sb.append(LOG_NAME);
		sb.append(",");
		sb.append(LOG_MESSAGE);
		sb.append(") values (?,?,?,?,?)");
		return connection.prepareStatement(sb.toString());
	}
	
	private void flushInserts() throws Exception {
		if (ps != null) {
			if (messageCountInBatch > 0) {
				// is there something left to do
				if (batchPeriodMillis > 0) {
					// messages in the batch
					ps.executeBatch();
				}
				// commit the rest
				if (connection.getAutoCommit() == false) {
					connection.commit();
				}
				messageCountInBatch = 0;
			}
		}
	}

	private void insertLogEntry(LogEntry e) throws Exception {
		if (ps == null) {
			ps = createPreparedStatement();
		}
		try {
			ps.setLong(1, jobInstanceId);
			ps.setTimestamp(2, new Timestamp(e.logTimestamp));
			ps.setString(3, e.level);
			ps.setString(4, e.loggerName);
			ps.setString(5, JobInstanceHelper.limitMessage(e.logMessage, maxMessageLength, 2));
			messageCountInBatch++;
			if (batchPeriodMillis > 0) {
				ps.addBatch();
				if (executeNow || messageCountInBatch >= maxMessagesUntilUpdate) {
					ps.executeBatch();
					if (connection.getAutoCommit() == false) {
						connection.commit();
					}
					executeNow = false;
					messageCountInBatch = 0;
				}
			} else {
				ps.executeUpdate();
				if (messageCountInBatch >= maxMessagesUntilUpdate) {
					if (connection.getAutoCommit() == false) {
						connection.commit();
					}
					messageCountInBatch = 0;
				}
			}
		} catch (SQLException sqle) {
			SQLException ne = sqle.getNextException();
			if (ne != null) {
				throw new Exception(sqle.getMessage() + ", next:" + ne.getMessage(), sqle);
			}
			if (connection.getAutoCommit() == false) {
				connection.rollback();
			}
			throw sqle;
		}
	}
	
	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	
	public void closeConnection() {
		if (this.connection != null) {
			try {
				if (connection.isClosed() == false) {
					connection.close();
				}
			} catch (Exception e) {}
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		if (tableName != null && tableName.trim().isEmpty() == false) {
			this.tableName = tableName;
		}
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getLogLevel() {
		return logLevel;
	}

	public void setLogLevel(String logLevel) {
		this.logLevel = logLevel;
	}

	public int getMaxMessageLength() {
		return maxMessageLength;
	}

	public void setMaxMessageLength(int maxMessageLength) {
		this.maxMessageLength = maxMessageLength;
	}

	@Override
	public void close() {
		stopWriterThread();
		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {}
			ps = null;
		}
		// connection will be closed from tJobInstanceEnd component
	}

	@Override
	public boolean requiresLayout() {
		return false;
	}

	@Override
	protected void append(LoggingEvent event) {
		if (closed == false) {
			// filter own events
			if (event.getMDC("jobInstanceId") != null && event.getMDC("jobInstanceId").equals(jobInstanceId)) {
				LogEntry e = new LogEntry();
				e.level = event.getLevel().toString();
				e.logTimestamp = event.timeStamp;
				if (layout != null) {
					e.logMessage = layout.format(event);
				} else {
					e.logMessage = event.getRenderedMessage();
				}
				e.loggerName = event.getLoggerName();
				if (dequeue.offerLast(e) == false) {
					System.err.println("Message queue is full, message will not persist");
				}
				checkWriter();
			}
		}
	}
	
	public long getJobInstanceId() {
		return jobInstanceId;
	}

	public void setJobInstanceId(long jobInstanceId) {
		this.jobInstanceId = jobInstanceId;
	}

	public Map<String, String> getAlternativeColumnNames() {
		return alternativeColumnNames;
	}

	public void setAlternativeColumnNames(Map<String, String> alternativeColumnNames) {
		this.alternativeColumnNames = alternativeColumnNames;
	}
	
	private String getColumn(String originalName) {
		String newName = alternativeColumnNames.get(originalName.toLowerCase());
		if (newName != null) {
			return newName;
		} else {
			return originalName;
		}
	}
	
	public void setLayout(Layout layout) {
		this.layout = layout;
	}

	public int getBatchPeriodMillis() {
		return batchPeriodMillis;
	}

	public void setBatchPeriodMillis(Integer batchPeriodMillis) {
		if (batchPeriodMillis != null) {
			this.batchPeriodMillis = batchPeriodMillis;
		}
	}

	public int getMaxMessagesUntilUpdate() {
		return maxMessagesUntilUpdate;
	}

	public void setMaxMessagesUntilUpdate(Integer maxMessagesUntilUpdate) {
		if (maxMessagesUntilUpdate != null) {
			this.maxMessagesUntilUpdate = maxMessagesUntilUpdate;
		}
	}

}
