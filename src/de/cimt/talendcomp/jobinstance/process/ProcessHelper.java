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
package de.cimt.talendcomp.jobinstance.process;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessHelper {
	
	private boolean isUnix = false;
	private boolean isWindows = false;
	
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
			throw new Exception("OS not recognized!");
		}
	}
	
	public boolean isUnix() {
		return isUnix;
	}
	
	public boolean isWindows() {
		return isWindows;
	}
	
	public List<Integer> retrieveProcessListForUnix() throws Exception {
		List<Integer> pids = new ArrayList<Integer>();
		ProcessBuilder pb = new ProcessBuilder("ps", "-eo", "pid");
		Process process = pb.start();
		BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
		String line = null;
		Pattern pattern = Pattern.compile("[0-9]{1,6}");
		while ((line = br.readLine()) != null) {
			line = line.trim();
			Matcher m = pattern.matcher(line);
			if (m.matches()) {
				int pid = Integer.parseInt(line);
				if (pid > 1) {
					pids.add(pid);
				}
			}
		}
		br.close();
		return pids;
	}

	public List<Integer> retrieveProcessListForWindows() throws Exception {
		List<Integer> pids = new ArrayList<Integer>();
		ProcessBuilder pb = new ProcessBuilder("tasklist", "/fo", "list");
		Process process = pb.start();
		BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
		String line = null;
		Pattern pattern = Pattern.compile("PID[:\\s]*([0-9]{1,6})", Pattern.CASE_INSENSITIVE);
		while ((line = br.readLine()) != null) {
			line = line.trim();
			Matcher m = pattern.matcher(line);
			if (m.matches()) {
				String pidStr = m.group(1);
				int pid = Integer.parseInt(pidStr);
				if (pid > 1) {
					pids.add(pid);
				}
			}
		}
		br.close();
		return pids;
	}
	
}
