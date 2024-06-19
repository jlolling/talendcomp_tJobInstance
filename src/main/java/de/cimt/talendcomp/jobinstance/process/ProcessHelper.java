/**
 * Copyright 2023 Jan Lolling jan.lolling@gmail.com
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
package de.cimt.talendcomp.jobinstance.process;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProcessHelper {
	
	private static Logger LOG = LogManager.getLogger(ProcessHelper.class);
	private boolean isUnix = false;
	private boolean isWindows = false;
	private String unixCommand = "ps -eo pid=";
	private String unixPidPattern = "([0-9]{1,9})";
	private String windowsCommand = "tasklist /fo list";
	private String windowsPidPattern = "PID[:\\s]*([0-9]{1,9})";
	
	public ProcessHelper() {}
	
	public void init() throws Exception {
		String os = System.getProperty("os.name");
		if (os != null) {
			os = os.toLowerCase().trim();
		} else {
			throw new Exception("Cannot determine operating system!");
		}
		if (os.contains("win")) {
			isUnix = false;
			isWindows = true;
		} else {
			// everything else than Windows is a UNIX system
			isUnix = true;
			isWindows = false;
		}
	}
	
	public List<Integer> retrieveProcessList() throws Exception {
		if (isUnix) {
			return retrieveProcessList(unixCommand, unixPidPattern);
		} else if (isWindows) {
			return retrieveProcessList(windowsCommand, windowsPidPattern);
		} else {
			throw new Exception("OS could not be recognized! Environment os.name=" + System.getProperty("os.name"));
		}
	}
	
	public boolean isUnix() {
		return isUnix;
	}
	
	public boolean isWindows() {
		return isWindows;
	}
	
	private List<String> getCommandAsList(String command) {
		if (command == null || command.trim().isEmpty()) {
			throw new IllegalArgumentException("command cannot be null or empty");
		}
		String[] array = command.split("\\s");
		List<String> cl = new ArrayList<String>();
		for (String part : array) {
			if (part != null && part.trim().isEmpty() == false) {
				cl.add(part.trim());
			}
		}
		return cl;
	}
	
	public List<Integer> retrieveProcessList(String command, String patternStr) throws Exception {
		LOG.info("Retrieve PIDs with command: '" + command + "' and regex: '" + patternStr + "'");
		List<Integer> pids = new ArrayList<Integer>();
		ProcessBuilder pb = new ProcessBuilder(getCommandAsList(command));
		Process process = null;
		try {
			process = pb.start();
		} catch (Exception ioe) {
			throw new Exception("Command to get PID list: '" + command + "' failed: " + ioe.getMessage(), ioe);
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
		String line = null;
		Pattern patternPid = Pattern.compile(patternStr);
		StringBuilder psCommandResponse = new StringBuilder();
		int receivedLines = 0;
		while ((line = br.readLine()) != null) {
			receivedLines++;
			line = line.trim();
			psCommandResponse.append(line);
			psCommandResponse.append("\n");
			if (line.isEmpty()) {
				continue;
			}
			Matcher m = patternPid.matcher(line);
			if (m.find()) {
				if (m.groupCount() == 0) {
					throw new Exception("The regex to find the PIDs must have at least one group (only the first group will be used). regex: " + patternStr + " command: " + command + " current line: " + line);
				}
				if (m.start() < m.end()) {
	            	String pidStr = m.group(1);
					int pid = Integer.parseInt(pidStr);
					if (pid > 1) {
						pids.add(pid);
					}
	            }
			}
		}
		br.close();
		if (receivedLines == 0) {
			throw new Exception("The command: '" + command + "' to find PIDs does not have any response");
		}
		if (pids.isEmpty()) {
			throw new Exception("No pids could be extracted by command: '" + command + "' using pattern: '" + patternStr + "' response:\n" + psCommandResponse);
		}
		return pids;
	}

	public String getUnixCommand() {
		return unixCommand;
	}

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

	public String getWindowsCommand() {
		return windowsCommand;
	}

	public void setWindowsCommand(String windowsCommand) {
		if (windowsCommand != null && windowsCommand.trim().isEmpty() == false) {
			this.windowsCommand = windowsCommand;
		}
	}

	public String getWindowsPidPattern() {
		return windowsPidPattern;
	}

	public void setWindowsPidPattern(String windowsPidPattern) {
		if (windowsPidPattern != null && windowsPidPattern.trim().isEmpty() == false) {
			this.windowsPidPattern = windowsPidPattern;
		}
	}
	
}
