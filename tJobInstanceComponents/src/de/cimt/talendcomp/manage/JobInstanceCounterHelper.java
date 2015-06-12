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
package de.cimt.talendcomp.manage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class JobInstanceCounterHelper {
	
	private long jobInstanceId = 0;
	private Connection connection;
	private String schemaName = null;
	private Map<String, Integer> counters = new HashMap<String, Integer>();
	public static final String JOB_INSTANCE_COUNTERS = "JOB_INSTANCE_COUNTERS";
	private static final String COUNTER_NAME = "COUNTER_NAME";
	private static final String COUNTER_VALUE = "COUNTER_VALUE";
	private String tableName = JOB_INSTANCE_COUNTERS;
	private Map<String, String> alternativeColumnNames = new HashMap<String, String>();

	private String getTable() {
		return schemaName != null ? schemaName + "." + tableName : tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void setSchemaName(String schemaName) {
		if (schemaName != null && schemaName.trim().isEmpty() == false) {
			this.schemaName = schemaName;
		}
	}

	public void addToCounter(String key, Number value) {
		if (key != null && value != null) {
			Integer pv = counters.get(key);
			if (pv != null) {
				value = value.intValue() + pv.intValue();
			}
			counters.put(key, value.intValue());
		}
	}

	public void subToCounter(String key, Number value) {
		if (key != null && value != null) {
			Integer pv = counters.get(key);
			if (pv != null) {
				value = pv.intValue() - value.intValue();
			}
			counters.put(key, value.intValue());
		}
	}

	public long getJobInstanceId() {
		return jobInstanceId;
	}

	public void setJobInstanceId(long jobInstanceId) {
		this.jobInstanceId = jobInstanceId;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		if (connection == null) {
			throw new IllegalArgumentException("connection cannot be null");
		}
		this.connection = connection;
	}
	
	public void writeCounters() throws Exception {
		if (counters.isEmpty() == false) {
			StringBuilder sb = new StringBuilder();
			sb.append("insert into ");
			sb.append(getTable());
			sb.append(" (");
			sb.append(getColumn(JobInstanceHelper.JOB_INSTANCE_ID));
			sb.append(",");
			sb.append(COUNTER_NAME);
			sb.append(",");
			sb.append(COUNTER_VALUE);
			sb.append(") values (?,?,?)");
			boolean hasValues = false;
			try {
				PreparedStatement ps = connection.prepareStatement(sb.toString());
				for (Map.Entry<String, Integer> entry : counters.entrySet()) {
					Integer value = entry.getValue();
					if (value != null) {
						ps.setLong(1, jobInstanceId);
						ps.setString(2, entry.getKey());
						ps.setInt(3, value);
						ps.addBatch();
						hasValues = true;
					}
				}
				if (hasValues) {
					ps.executeBatch();
					if (connection.getAutoCommit() == false) {
						connection.commit();
					}
					ps.close();
				}
			} catch (SQLException sqle) {
				SQLException ne = sqle.getNextException();
				if (ne != null) {
					throw new Exception(sqle.getMessage() + ", Next Exception:" + ne.getMessage(), sqle);
				} else {
					throw sqle;
				}
			}
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

	public Map<String, String> getAlternativeColumnNames() {
		return alternativeColumnNames;
	}

	public void setAlternativeColumnNames(Map<String, String> alternativeColumnNames) {
		this.alternativeColumnNames = alternativeColumnNames;
	}
	
}
