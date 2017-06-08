package de.cimt.talendcomp.jobinstance.manage;

import java.lang.management.ManagementFactory;

/**
 * 64:    Das oberste Bit wird immer 0 gesetzt um eine positive Ganzzahl zu behalten
 * 63-31: die letzten 33 Bits der Unix Time (reicht bis zum Jahr 2241,  und ist aufrund der enthaltenen Unix Pid auch Schaltsekundenfest)
 * 30-22: 9 Bit Hostindex (Es können maximal 512 Hosts parallel genutzt werden)
 * 21-10: die letzten 12 Bit der Unix Pid ( es dürfen maximal 4.096 Prozesse innerhalb der gleichen Sekunde gestartet werden)
 * 9-1:   9 Bit Subjob count (Es können maximal 512 Threads innerhalb einer PID und einer Sekunde gestartet werden)
 *
 * 64:    Das oberste Bit wird immer 0 gesetzt um eine positive Ganzzahl zu behalten
 * 63-17: die letzten 46 Bits der Unix Time in ms
 * 16-1:  UNIX-PID
 * 
 * @author jan.lolling@gmail.com
 *
 */
public class JID1 {
	
	private short hostIndex = 0;
	private Long startDate = null;
	private long unixTime = 0l;
	private long lastUnixTime = 0l;
	private long unixPid;
	private static short subJobIndex = 0;
	private long jid;
	
	public static final long mask33 =        Long.parseLong("111111111111111111111111111111111", 2);
	public static final long mask12 =        Long.parseLong("111111111111", 2);
	public static final long maskHostIndex = Long.parseLong("00000000000000000000000000000000111111111000000000000000000000", 2);
	public static final long maskUnixPid =   Long.parseLong("00000000000000000000000000000000000000000111111111111000000000", 2);
	public static final long maskSubJob =    Long.parseLong("00000000000000000000000000000000000000000000000000000111111111", 2);
	
	public long createJID() throws Exception {
		unixTime = retrieveTimeInSec();
		// 33  bit mask
		jid = unixTime & mask33;
		jid = jid << 30;
		long hostindex = retrieveHostIndex();
		hostindex = hostindex << 21;
		jid = jid | hostindex;
		long unixpid = retrievePid();
		long unixPid0 = unixpid & mask12;
		unixPid0 = unixPid << 9;
		jid = jid | unixPid0;
		long subjobindex = retrieveSubjobIndex();
		jid = jid | subjobindex;
		lastUnixTime = unixTime;
		return jid;
	}
	
	public long getTimePart(long jobInstanceId) {
		return jobInstanceId >> 30;
	}
	
	public long getHostIndex(long jobInstanceId) {
		return (jobInstanceId & maskHostIndex) >> 21;
	}
	
	public long getUnixPid(long jobInstanceId) {
		return (jobInstanceId & maskUnixPid) >> 9;
	}
	
	private long retrieveTimeInSec() {
		if (startDate != null) {
			return startDate / 1000l;
		} else {
			return System.currentTimeMillis() / 1000l;
		}
	}
	
	private long retrieveHostIndex() {
		return hostIndex;
	}

	private long retrieveSubjobIndex() {
		if (subJobIndex == Short.MAX_VALUE) {
			subJobIndex = 0;
			return Short.MAX_VALUE;
		} else {
			if (unixTime > lastUnixTime) {
				subJobIndex = 0;
			}
			return subJobIndex++;
		}
	}

	private long retrievePid() throws Exception {
		if (unixPid == 0) {
			String processInfo = ManagementFactory.getRuntimeMXBean().getName();
			int p = processInfo.indexOf('@');
			if (p > 0) {
				unixPid = p;
				return p;
			} else {
				throw new Exception("Runtime MXBean does not contains PID");
			}
		} else {
			return unixPid;
		}
	}

	public void setHostIndex(short hostIndex) {
		this.hostIndex = hostIndex;
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

	public void setUnixPid(Integer pid) {
		if (pid != null) {
			this.unixPid = pid;
		}
	}

}