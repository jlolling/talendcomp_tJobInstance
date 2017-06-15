package de.cimt.talendcomp.jobinstance.manage;

/**
 * 64:    Das oberste Bit wird immer 0 gesetzt um eine positive Ganzzahl zu behalten
 * 63-13: die letzten 51 Bits der Unix Time in ms
 * 12-1:  Sequence innerhalb der Millisekunde mit zufälligem Offset im Bereich von 0-1000
 * 
 * @author jan.lolling@gmail.com
 *
 */
public class JID {
	
	private Long startDate = null;
	private long currentMillisecond = 0;
	private long lastMillisecond = 0;
	private long jid;
	private int sequenceValue = 1;
	private final long TIME_OFFSET = 946681200000l; // 2000-01-01 00:00:00
	
	public static final long mask51 =  Long.parseLong("111111111111111111111111111111111111111111111111111", 2);
	public static final int mask12 = Integer.parseInt("111111111111", 2);
	
	public long createJID() throws Exception {
		currentMillisecond = retrieveTimeInMillis();
		// 33  bit mask
		sequenceValue = setupSequenceWithinMilliSec();
		sequenceValue = sequenceValue & mask12;
		jid = currentMillisecond & mask51;
		jid = jid << 12;
		jid = jid | sequenceValue;
		lastMillisecond = currentMillisecond;
		return jid;
	}
	
	public long getTimePart(long jobInstanceId) {
		return jobInstanceId >> 12;
	}
	
	private long retrieveTimeInMillis() {
		if (startDate != null) {
			return startDate - TIME_OFFSET;
		} else {
			return System.currentTimeMillis() - TIME_OFFSET;
		}
	}

	public static String longToString(long number) {
	    StringBuilder result = new StringBuilder();
	    for (int i = 63; i >= 0 ; i--) {
	        long mask = 1l << i;
	        result.append((number & mask) != 0 ? "1" : "0");
	        if (i % 4 == 0) {
	            result.append(" ");
	        }
	    }
	    result.replace(result.length() - 1, result.length(), "");
	    return result.toString();
	}
	
	public long getJID() {
		return jid;
	}
	
	@Override
	public String toString() {
		return jid + " bits: " + longToString(jid);
	}

	public void setStartDate(Long startDate) {
		if (startDate != null) {
			this.startDate = startDate;
		}
	}

	public synchronized int setupSequenceWithinMilliSec() throws InterruptedException {
		if (lastMillisecond < currentMillisecond) {
			sequenceValue = (int) (Math.random() * 100);
		} else {
			sequenceValue = sequenceValue + 1;
		}
		if (sequenceValue >= mask12) {
			Thread.sleep(1);
			// rerun getting the current time
			currentMillisecond = retrieveTimeInMillis();
			sequenceValue = 1;
		}
		return sequenceValue;
	}

	public long getCurrentMillisecond() {
		return currentMillisecond + TIME_OFFSET;
	}

	public int getSequenceValue() {
		return sequenceValue;
	}

}