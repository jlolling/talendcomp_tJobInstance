package de.cimt.talendcomp.log4j;

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