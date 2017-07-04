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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.FileAppender;
import org.apache.log4j.MDC;
import org.apache.log4j.spi.LoggingEvent;

public class TalendJobFileAppender extends FileAppender {
	
	private SimpleDateFormat date_param_format = new SimpleDateFormat("yyyyMMdd_HHmmss");
	private Map<String, Object> jobAttributes = new HashMap<String, Object>();
	
	public TalendJobFileAppender() {
		super();
	}
	
	@Override
	public void append(LoggingEvent event) {
		if (filter(event)) {
			super.append(event);
		}
	}

	private boolean filter(LoggingEvent event) {
		String attrPid = (String) jobAttributes.get("talendPid");
		if (attrPid != null) {
			return attrPid.equals(MDC.get("talendPid"));
		} else {
			return true;
		}
	}
	
	@Override
	public void setFile(String file) {
		if (file == null || file.trim().isEmpty()) {
			throw new IllegalArgumentException("file cannot be null or empty");
		}
		String logFilePath = replacePlaceholders(file);
		File dir = new File(logFilePath).getParentFile();
		if (dir.exists() == false) {
			dir.mkdirs();
		}
		if (dir.exists() == false) {
			throw new RuntimeException("Cannot create log dir: " + dir.getAbsolutePath());
		}
		super.setFile(logFilePath);
		setBufferedIO(false);
		setImmediateFlush(true);
		setName(getFile());
	}
		
	public void setJobAttribute(String key, Object value) {
		if (key == null || key.trim().isEmpty()) {
			throw new IllegalArgumentException("key cannot be null");
		}
		if (value != null) {
			jobAttributes.put(key, value);
		}
	}
	
	private String replacePlaceholders(String template) {
		for (Map.Entry<String, Object> entry : jobAttributes.entrySet()) {
			template = template.replace("{" + entry.getKey() + "}", convertValue(entry.getValue()));
		}
		return template;
	}

	private String convertValue(Object value) {
		if (value != null) {
			if (value instanceof Date) {
				return date_param_format.format((Date) value);
			} else if (value instanceof String) {
				return (String) value;
			} else {
				return value.toString();
			}
		} else {
			return "";
		}
	}
	
}
