package de.cimt.talendcomp.process;

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
		if (os.contains("linux") || os.contains("mac")) {
			isUnix = true;
			isWindows = false;
		} else if (os.contains("win")) {
			isUnix = false;
			isWindows = true;
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
