package de.cimt.talendcomp.jobinstance.manage;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ErrorCodeEvaluator {
	
	public static class Rule {
		
		static final String PREFIX = "EXITCODE_";
		int exitCode = 0;
		String regex = null;
		Pattern pattern = null;
		
		Rule(int exitCode, String regex) throws Exception {
			this.exitCode = exitCode;
			try {
				pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
			} catch (Exception e) {
				throw new Exception("Compile regex for exitCode=" + exitCode + " with regex=" + regex + " failed: " + e.getMessage(), e);
			}
		}
		
		public static Rule build(String line) throws Exception {
			if (line != null && line.trim().isEmpty() == false) {
				line = line.trim();
				if (line.startsWith("#") == false) {
					int pos = line.indexOf('=');
					if (pos != -1) {
						String key = line.substring(0, pos).trim();
						if (key.isEmpty()) {
							throw new Exception("configuration line invalid because no key!");
						} else if (key.startsWith(PREFIX)) {
							String iv = key.substring(PREFIX.length());
							if (iv != null && iv.trim().isEmpty() == false) {
								int exitCode;
								try {
									exitCode = Integer.valueOf(iv.trim());
								} catch (Exception pe) {
									throw new Exception("Cannot extract an exitCode from key: " + key + " in line: " + line);
								}
								String value = line.substring(pos + 1).trim();
								if (value.isEmpty() == false) {
									Rule rule = new Rule(exitCode, value);
									return rule;
								}
							}
							
						}
					}
				}
			}
			return null;
		}
		
	}
	
	private List<Rule> rules = new ArrayList<>();
	
	public void addRule(Integer exitCode, String regex) throws Exception {
		if (exitCode != null && regex != null && regex.trim().isEmpty() == false) {
			rules.add(new Rule(exitCode, regex));
		}
	}
	
	public void readRulesFromFile(String propertiesFilePath) throws Exception {
		if (propertiesFilePath != null) {
			File f = new File(propertiesFilePath);
			if (f.canRead() == false) {
				throw new Exception("Rule config file: " + f.getAbsolutePath() + " could not be read or does not exist.");
			}
			List<String> lines = Files.readAllLines(f.toPath(), Charset.forName("ISO-8859-1"));
			for (String line : lines) {
				Rule rule = Rule.build(line);
				if (rule != null) {
					rules.add(rule);
				}
			}
		}
	}
	

}
