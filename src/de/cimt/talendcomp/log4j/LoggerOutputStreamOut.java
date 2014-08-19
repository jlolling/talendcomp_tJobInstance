package de.cimt.talendcomp.log4j;

import java.io.IOException;
import java.io.PrintStream;

public class LoggerOutputStreamOut extends LoggerOutputStream {

	public LoggerOutputStreamOut() {
		super();
	}
	
	public LoggerOutputStreamOut(PrintStream delegate, boolean forward) {
		super(delegate, forward);
	}

	@Override
	protected String getChannel() {
		return "out>";
	}

	@Override
	protected int getLevel() {
		return 3; // INFO
	}

	@Override
	public void close() throws IOException {
		if (delegate != null) {
			try {
				delegate.flush();
			} catch (Throwable t) {}
			System.setOut(delegate);
		}
		super.close();
	}

}
