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
package de.cimt.talendcomp.jobinstance.manage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class JobInstanceContextHelper {

	private final static Logger logger = null;
	private Connection connection;
	private Map<String, String> attributeMap = new HashMap<String, String>();
	private NumberFormat numberFormat = null;
	private String schemaName = null;
	
	public JobInstanceContextHelper() {
		numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
		numberFormat.setGroupingUsed(false);
	}

	public void setAttribute(String key, String value, boolean keepLastValue) {
		if (key == null || key.trim().isEmpty()) {
			throw new IllegalArgumentException("key cannot be null or empty");
		}
		if (value != null) {
			if (isDebug()) {
				debug("   add context variable " + key + "=" + value);
			}
			attributeMap.put(key.trim(), value);
		} else {
			if (keepLastValue == false) {
				attributeMap.remove(key);
			}
		}
	}
	
	public Set<String> getAttributeNames() {
		return attributeMap.keySet();
	}
	
	public Object getAttribute(String key, String clazz) throws Exception {
		if (key == null || key.trim().isEmpty()) {
			throw new IllegalArgumentException("key cannot be null or empty");
		}
		return getValueObject(attributeMap.get(key), clazz);
	}
	
	private Object getValueObject(String value, String clazz) throws Exception {
		if (value == null) {
			return null;
		}
		try {
			if (clazz != null) {
				if (clazz.contains("String")) {
					return value;
				} else if (clazz.contains("Boolean")) {
					return Boolean.parseBoolean(value);
				} else if (clazz.contains("Short")) {
					return Short.parseShort(value);
				} else if (clazz.contains("Integer")) {
					return Integer.parseInt(value);
				} else if (clazz.contains("Long")) {
					return Long.parseLong(value);
				} else if (clazz.contains("Double")) {
					return numberFormat.parse(value);
				} else if (clazz.contains("Float")) {
					return numberFormat.parse(value);
				} else if (clazz.contains("BigDecimal")) {
					return numberFormat.parse(value);
				} else if (clazz.contains("Date")) {
					return GenericDateUtil.parseDate(value, "yyyy-MM-dd HH:mm:ss");
				} else if (clazz.contains("Timestamp")) {
					return new Timestamp(GenericDateUtil.parseDate(value, "yyyy-MM-dd HH:mm:ss").getTime());
				} else {
					return value;
				}
			} else {
				return value;
			}
		} catch (ParseException pe) {
			throw new Exception("Parse context value string: " + value + " to class: " + clazz + " failed: " + pe.getMessage(), pe);
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
	
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public void clear() {
		attributeMap.clear();
	}

	private String getSchemaPrefix() {
		if (schemaName != null && schemaName.trim().isEmpty() == false) {
			return schemaName + ".";
		} else {
			return "";
		}
	}

	public void loadContext(String taskName) throws Exception {
		if (taskName == null || taskName.trim().isEmpty()) {
			throw new IllegalArgumentException("taskName cannot be null or empty");
 		}
		String sql = "select\n"
			    + "    parameter_name,\n"
			    + "    parameter_value\n"
			    + "from " + getSchemaPrefix() + "job_parameter_values"
			    + "where task_name = ?";
		debug(sql);
		PreparedStatement query = connection.prepareStatement(sql);
		query.setString(1, taskName);
		ResultSet rs = query.executeQuery();
		while (rs.next()) {
			String key = rs.getString(1);
			String value = rs.getString(2);
			setAttribute(key, value, false);	
		}
		rs.close();
		query.close();
		if (connection.getAutoCommit() == false) {
			connection.commit();
		}
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
	
	public static boolean isDebug() {
		return (logger != null && logger.isDebugEnabled()) || JobInstanceHelper.debug;
	}

}