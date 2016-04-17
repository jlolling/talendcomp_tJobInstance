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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

public abstract class LoggerOutputStream extends OutputStream {
	 
    private StringBuilder buffer;
    protected PrintStream delegate = null;
    protected boolean forward = false;
 
    private Logger logger = Logger.getLogger("talend");
 
    public LoggerOutputStream(PrintStream delegate, boolean forward) {
    	this.delegate = delegate;
    	if (delegate != null) {
        	try {
        		delegate.flush();
        	} catch (Throwable t) {}
    	}
        this.buffer = new StringBuilder(100);
        this.forward = forward;
    }
    
    public LoggerOutputStream() {
        this.buffer = new StringBuilder(100);
    }
 
    protected abstract String getChannel(); 
    
    protected abstract int getLevel();
    
    @Override
    public void write(final int b) {
        char c = (char) b;
        if (c == '\n') {
        	String message = buffer.toString();
        	if (message.trim().isEmpty() == false) {
            	if (forward && delegate != null) {
            		try {
            			delegate.println(message);
            		} catch (Throwable t) {
            			logger.error(t);
            		}
            	}
            	// prevent loops
            	if (message.contains(getChannel()) == false) {
            		MDC.put("origin", "System");
            		switch (getLevel()) {
            		case 6:  // fatal
            			logger.fatal(getChannel() + message);
            			break;
            		case 5:  // error
            			logger.error(getChannel() + message);
            			break;			
            		case 4:  // warn
            			logger.warn(getChannel() + message);
            			break;
            		case 3:  // info
            			logger.info(getChannel() + message);
            			break;
            		case 2:  // debug
            			logger.debug(getChannel() + message);
            			break;
            		case 1:  // trace
            			logger.trace(getChannel() + message);
            			break;
            		default:
            			throw new IllegalArgumentException("Unknown priority: " + getLevel());
            		} 
            	}
        	}
            buffer.setLength(0);
        } else {
            buffer.append(c);
        }
    }

	@Override
	public void flush() throws IOException {
		if (delegate != null) {
			try {
				delegate.flush();
			} catch (Throwable t) {}
		}
		super.flush();
	}

}