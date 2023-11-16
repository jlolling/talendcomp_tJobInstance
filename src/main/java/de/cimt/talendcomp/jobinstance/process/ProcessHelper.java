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

import jdk.internal.org.jline.utils.Log;

public class ProcessHelper {
	
	private static Logger LOG = LogManager.getLogger(ProcessHelper.class);
	private boolean isUnix = false;
	private boolean isWindows = false;
	private String unixCommand = "ps -eo pid";
	private String unixPidPattern = "[0-9]{1,8}";
	private String windowsCommand = "tasklist /fo list";
	private String windowsPidPattern = "PID[:\\s]*([0-9]{1,6})";
	
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
			return retrieveProcessListForUnix();
		} else if (isWindows) {
			return retrieveProcessListForWindows();
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
	
	public List<Integer> retrieveProcessListForUnix() throws Exception {
		Log.info("Retrieve Unix PIDs with command: '" + unixCommand + "' and regex: '" + unixPidPattern + "'");
		List<Integer> pids = new ArrayList<Integer>();
		ProcessBuilder pb = new ProcessBuilder(unixCommand);
		Process process = pb.start();
		BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
		String line = null;
		Pattern patternPid = Pattern.compile(unixPidPattern);
		StringBuilder psCommandResponse = new StringBuilder();
		while ((line = br.readLine()) != null) {
			line = line.trim();
			psCommandResponse.append(line);
			psCommandResponse.append("\n");
			Matcher m = patternPid.matcher(line);
			if (m.find()) {
				int pid = Integer.parseInt(line);
				if (pid > 1) {
					pids.add(pid);
				}
			}
		}
		br.close();
		if (pids.isEmpty()) {
			LOG.error("No pids could be extracted by unix command: '" + unixCommand + "' using pattern: '" + unixPidPattern + "' response:\n" + psCommandResponse);
		}
		return pids;
	}

	public List<Integer> retrieveProcessListForWindows() throws Exception {
		Log.info("Retrieve Windows PIDs with command: '" + windowsCommand + "' and regex: '" + windowsPidPattern + "'");
		List<Integer> pids = new ArrayList<Integer>();
		ProcessBuilder pb = new ProcessBuilder(windowsCommand);
		Process process = pb.start();
		BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
		String line = null;
		Pattern pattern = Pattern.compile(windowsPidPattern, Pattern.CASE_INSENSITIVE);
		StringBuilder psCommandResponse = new StringBuilder();
		while ((line = br.readLine()) != null) {
			line = line.trim();
			psCommandResponse.append(line);
			psCommandResponse.append("\n");
			Matcher m = pattern.matcher(line);
			if (m.find()) {
				String pidStr = m.group(1);
				int pid = Integer.parseInt(pidStr);
				if (pid > 1) {
					pids.add(pid);
				}
			}
		}
		br.close();
		if (pids.isEmpty()) {
			LOG.error("No pids could be extracted by windows command: '" + windowsCommand + "' using pattern: '" + windowsPidPattern + "' response:\n" + psCommandResponse);
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
