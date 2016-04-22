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
