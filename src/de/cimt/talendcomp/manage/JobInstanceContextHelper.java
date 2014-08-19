package de.cimt.talendcomp.manage;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class JobInstanceContextHelper {

	private Connection connection;
	private long jobInstanceId = 0;
	public static final String JOB_INSTANCE_CONTEXT = "JOB_INSTANCE_CONTEXT";
	private static final String ATTRIBUTE_KEY = "ATTRIBUTE_KEY";
	private static final String ATTRIBUTE_VALUE = "ATTRIBUTE_VALUE";
	private static final String ATTRIBUTE_TYPE = "ATTRIBUTE_TYPE";
	private static final String IS_OUTPUT_ATTR = "IS_OUTPUT_ATTR";
	private Map<String, Object> attributeMap = new HashMap<String, Object>();
	private Map<String, String> attributeClassMap = new HashMap<String, String>();
	private NumberFormat numberFormat = null;
	private SimpleDateFormat dateFormat = null;
	private String tableName = null;
	private String schemaName = null;
	private boolean isOutput = false;
	private Map<String, String> alternativeColumnNames = new HashMap<String, String>();
	
	public JobInstanceContextHelper() {
		numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
		numberFormat.setGroupingUsed(false);
		dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // this is compatible to the Talend way
	}

	private String getTable() {
		if (tableName == null) {
			tableName = JOB_INSTANCE_CONTEXT;
		}
		if (schemaName != null) {
			return schemaName + "." + tableName;
		} else {
			return tableName;
		}
	}
	
	public void setAttribute(String key, Object value, boolean keepLastValue) {
		if (key == null || key.isEmpty()) {
			throw new IllegalArgumentException("key cannot be null or empty");
		}
		if (value != null) {
			attributeMap.put(key, value);
			attributeClassMap.put(key, value.getClass().getName());
		} else {
			if (keepLastValue == false) {
				attributeMap.remove(key);
			}
		}
	}
	
	public Set<String> getAttributeNames() {
		return attributeMap.keySet();
	}
	
	public Object getAttribute(String key) {
		return attributeMap.get(key);
	}
	
	public String getAttributeClassName(String key) {
		return attributeClassMap.get(key);
	}

	public void writeContext() throws Exception {
		if (attributeMap.isEmpty() == false) {
			StringBuilder sb = new StringBuilder();
			sb.append("insert into ");
			sb.append(getTable());
			sb.append(" ("); 
		    sb.append(getColumn(JobInstanceHelper.JOB_INSTANCE_ID));
		    sb.append(",");
		    sb.append(ATTRIBUTE_KEY);
		    sb.append(",");
		    sb.append(ATTRIBUTE_VALUE);
		    sb.append(",");
		    sb.append(ATTRIBUTE_TYPE);
		    sb.append(",");
		    sb.append(IS_OUTPUT_ATTR);
		    sb.append(") values (?,?,?,?,?)");
			boolean hasValues = false;
			try {
				PreparedStatement ps = connection.prepareStatement(sb.toString());
				for (Map.Entry<String, Object> entry : attributeMap.entrySet()) {
					Object value = entry.getValue();
					if (value != null) {
						ps.setLong(1, jobInstanceId);
						ps.setString(2, entry.getKey());
						ps.setString(3, getValueString(value));
						ps.setString(4, getValueClass(value));
						ps.setBoolean(5, isOutput);
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
	
	private String getValueString(Object value) {
		if (value instanceof String) {
			return (String) value;
		} else if (value instanceof Boolean) {
			return Boolean.toString((Boolean) value);
		} else if (value instanceof Integer) {
			return Integer.toString((Integer) value);
		} else if (value instanceof Long) {
			return Long.toString((Long) value);
		} else if (value instanceof Double) {
			return numberFormat.format((Double) value);
		} else if (value instanceof Float) {
			return numberFormat.format((Float) value);
		} else if (value instanceof BigDecimal) {
			return numberFormat.format((BigDecimal) value);
		} else if (value instanceof Date) {
			return dateFormat.format((Date) value);
		}
		return value.toString();
	}
	
	private String getValueClass(Object value) {
		return value.getClass().getSimpleName();
	}
	
	public void readContext(Long jobInstanceId) throws Exception {
		if (jobInstanceId != null) {
			StringBuilder sb = new StringBuilder();
			sb.append("select ");
			sb.append(ATTRIBUTE_KEY);
			sb.append(",");
			sb.append(ATTRIBUTE_VALUE);
			sb.append(",");
			sb.append(ATTRIBUTE_TYPE);
			sb.append(" from ");
			sb.append(getTable());
			sb.append(" where ");
			sb.append(getColumn(JobInstanceHelper.JOB_INSTANCE_ID));
			sb.append("=");
			sb.append(jobInstanceId);
			Statement stat = connection.createStatement();
			ResultSet rs = stat.executeQuery(sb.toString());
			attributeMap.clear();
			while (rs.next()) {
				String key = rs.getString(1);
				String value = rs.getString(2);
				String clazz = rs.getString(3);
				setAttribute(key, getValueObject(value, clazz), false);
			}
			rs.close();
			stat.close();
		}
	}
	
	private Object getValueObject(String value, String clazz) throws ParseException {
		if ("String".equals(clazz)) {
			return value;
		} else if ("Boolean".equals(clazz)) {
			return Boolean.parseBoolean(value);
		} else if ("Short".equals(clazz)) {
			return Short.parseShort(value);
		} else if ("Integer".equals(clazz)) {
			return Integer.parseInt(value);
		} else if ("Long".equals(clazz)) {
			return Long.parseLong(value);
		} else if ("Double".equals(clazz)) {
			return (Double) numberFormat.parse(value);
		} else if ("Float".equals(clazz)) {
			return (Float) numberFormat.parse(value);
		} else if ("BigDecimal".equals(clazz)) {
			return (BigDecimal) numberFormat.parse(value);
		} else if ("Date".equals(clazz)) {
			return dateFormat.parse(value);
		} else if ("Timestamp".equals(clazz)) {
			return new Timestamp(dateFormat.parse(value).getTime());
		} else {
			return value;
		}
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
	
	public long getJobInstanceId() {
		return jobInstanceId;
	}
	
	public void setJobInstanceId(long jobInstanceId) {
		this.jobInstanceId = jobInstanceId;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public boolean isOutput() {
		return isOutput;
	}

	public void setOutput(boolean isOutput) {
		this.isOutput = isOutput;
	}
	
	public void clear() {
		attributeMap.clear();
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

}