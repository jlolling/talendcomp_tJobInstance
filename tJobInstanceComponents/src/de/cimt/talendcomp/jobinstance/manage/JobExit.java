package de.cimt.talendcomp.jobinstance.manage;

public class JobExit {
	
	private String message;
	private Integer code;

	public JobExit(String message, Integer exitCode) {
		if (message != null && message.trim().isEmpty() == false) {
			this.message = message.trim();
		}
		this.code = exitCode;
	}

	public String getMessage() {
		return message;
	}

	public Integer getCode() {
		return code;
	}
	
}
