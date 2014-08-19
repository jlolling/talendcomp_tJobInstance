package de.cimt.talendcomp.log4j;

import java.io.IOException;
import java.io.PrintStream;

public class LoggerOutputStreamErr extends LoggerOutputStream {

	public LoggerOutputStreamErr() {
		super();
	}
	
	public LoggerOutputStreamErr(PrintStream delegate, boolean forward) {
		super(delegate, forward);
	}

	@Override
	protected String getChannel() {
		return "err>";
	}

	@Override
	protected int getLevel() {
		return 5; // ERROR
	}

	@Override
	public void close() throws IOException {
		if (delegate != null) {
			try {
				delegate.flush();
			} catch (Throwable t) {}
			System.setErr(delegate);
		}
		super.close();
	}

}
