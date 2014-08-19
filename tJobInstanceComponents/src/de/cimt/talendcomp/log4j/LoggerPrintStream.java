package de.cimt.talendcomp.log4j;

import java.io.OutputStream;
import java.io.PrintStream;

public class LoggerPrintStream extends PrintStream {

	public LoggerPrintStream(OutputStream out) {
		super(out);
	}

}
