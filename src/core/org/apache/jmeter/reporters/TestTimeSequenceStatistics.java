package org.apache.jmeter.reporters;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * TestTimeSequenceStatistics.
 * 
 * @author jiangshaoyu
 */
public class TestTimeSequenceStatistics implements Serializable{

    private static final long serialVersionUID = 1L;
    private static final int JMETER_RESULT_ROW_SIZE = 7;
    
    private final AtomicLong testTime = new AtomicLong(0L);
    private final AtomicLong maxTestTime = new AtomicLong(0L);
    private final AtomicLong errors = new AtomicLong(0L);
    private final AtomicLong status1xx = new AtomicLong(0L);
    private final AtomicLong status2xx = new AtomicLong(0L);
    private final AtomicLong status3xx = new AtomicLong(0L);
    private final AtomicLong status4xx = new AtomicLong(0L);
    private final AtomicLong status5xx = new AtomicLong(0L);
    private final AtomicLong httpResponseLen = new AtomicLong(0L);
    private final AtomicLong httpResponseErrors = new AtomicLong(0L);
    private final AtomicLong timeToResolveHost = new AtomicLong(0L);
    private final AtomicLong maxTimeToResolveHost = new AtomicLong(0L);
    private final AtomicLong timeToEstabishConn = new AtomicLong(0L);
    private final AtomicLong maxTimeToEstabishConn = new AtomicLong(0L);
    private final AtomicLong timeToFirstByte = new AtomicLong(0L);
    private final AtomicLong maxTimeToFirstByte = new AtomicLong(0L);
    private final AtomicLong newConnections = new AtomicLong(0L);
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final Map<Long, AtomicInteger> allTestTime = new ConcurrentHashMap<Long, AtomicInteger>();
    
    public TestTimeSequenceStatistics(final CopyOnWriteArrayList<Long> longStatics) {
        if(longStatics.size() == JMETER_RESULT_ROW_SIZE) {
            this.errors.set(longStatics.get(3));
            this.requestCount.incrementAndGet();
            this.allTestTime.clear();
            this.allTestTime.put(longStatics.get(1), new AtomicInteger(1));
            countHttpRespCode(longStatics.get(2));
            this.testTime.set(longStatics.get(1));
            this.maxTestTime.set(longStatics.get(1));
            if(longStatics.get(3) == 0L) {
                this.httpResponseLen.set(longStatics.get(4));
                this.httpResponseErrors.set(longStatics.get(3));
                this.timeToEstabishConn.set(longStatics.get(6));
                this.maxTimeToEstabishConn.set(longStatics.get(6));
                this.timeToFirstByte.set(longStatics.get(5));
                this.maxTimeToFirstByte.set(longStatics.get(5));
            }
        }
    }
       
    public synchronized void combine(final CopyOnWriteArrayList<Long> longStatics){
        this.errors.addAndGet(longStatics.get(3));
        this.requestCount.incrementAndGet();
        countHttpRespCode(longStatics.get(2));
        this.testTime.addAndGet(longStatics.get(1));
        if(allTestTime.containsKey(longStatics.get(1))) {
            allTestTime.get(longStatics.get(1)).incrementAndGet();
        } else {
            allTestTime.put(longStatics.get(1), new AtomicInteger(1));
        }
        calMaxTestTime(longStatics.get(1));
        
        if(longStatics.get(3) == 0L) {
            this.httpResponseLen.addAndGet(longStatics.get(4));
            this.httpResponseErrors.addAndGet(longStatics.get(3));
            this.timeToEstabishConn.addAndGet(longStatics.get(6));
            calMaxTimeToEstabishConn(longStatics.get(6));
            this.timeToFirstByte.addAndGet(longStatics.get(5));
            calMaxTimeToFirstByte(longStatics.get(5));
        }
    }    
    
    private void calMaxTestTime(final long testTime) {
        if (testTime > this.maxTestTime.get() ) {
            this.maxTestTime.set(testTime);
        }
    }
    
    private void calMaxTimeToResolveHost(final long timeToResolveHost) {
        if(timeToResolveHost > this.maxTimeToResolveHost.get()) {
            this.maxTimeToResolveHost.set(timeToResolveHost);
        }
    }
    
    private void countHttpRespCode(final long httpRespCode) {
        if(httpRespCode<200L) {
            this.status1xx.incrementAndGet();
        }else if (httpRespCode<300L) {
            this.status2xx.incrementAndGet();
        }else if (httpRespCode<400L) {
            this.status3xx.incrementAndGet();
        }else if (httpRespCode<500L) {
            this.status4xx.incrementAndGet();
        }else if (httpRespCode<600L) {
            this.status5xx.incrementAndGet();
        }
    }
    
    private void calMaxTimeToEstabishConn(final long timeToEstablishConn) {
        if (timeToEstablishConn > this.maxTimeToEstabishConn.get()) {
            this.maxTimeToEstabishConn.set(timeToEstablishConn);
        }
    }
    
    private void calMaxTimeToFirstByte(final long maxTimeToFirstByte) {
        if(maxTimeToFirstByte > this.maxTimeToFirstByte.get()) {
            this.maxTimeToFirstByte.set(maxTimeToFirstByte);
        }
    }

    public AtomicLong getTestTime() {
        return testTime;
    }

    public AtomicLong getMaxTestTime() {
        return maxTestTime;
    }

    public AtomicLong getErrors() {
        return errors;
    }

    public AtomicLong getStatus1xx() {
        return status1xx;
    }

    public AtomicLong getStatus2xx() {
        return status2xx;
    }

    public AtomicLong getStatus3xx() {
        return status3xx;
    }

    public AtomicLong getStatus4xx() {
        return status4xx;
    }

    public AtomicLong getStatus5xx() {
        return status5xx;
    }

    public AtomicLong getHttpResponseLen() {
        return httpResponseLen;
    }

    public AtomicLong getHttpResponseErrors() {
        return httpResponseErrors;
    }

    public AtomicLong getTimeToResolveHost() {
        return timeToResolveHost;
    }

    public AtomicLong getMaxTimeToResolveHost() {
        return maxTimeToResolveHost;
    }

    public AtomicLong getTimeToEstabishConn() {
        return timeToEstabishConn;
    }

    public AtomicLong getMaxTimeToEstabishConn() {
        return maxTimeToEstabishConn;
    }

    public AtomicLong getTimeToFirstByte() {
        return timeToFirstByte;
    }

    public AtomicLong getMaxTimeToFirstByte() {
        return maxTimeToFirstByte;
    }
    
    public AtomicLong getNewConnections() {
        return newConnections;
    }

    public AtomicInteger getRequestCount() {
        return requestCount;
    }
    
    public String getTestTimes() {
    	final StringBuffer sb = new StringBuffer();
    	final Iterator<Entry<Long, AtomicInteger>> it = allTestTime.entrySet().iterator();
    	while(it.hasNext()) {
    		final Entry<Long, AtomicInteger> entry = it.next();
    		sb.append(entry.getKey());
    		sb.append(':');
    		sb.append(entry.getValue());
    		sb.append('|');
    	}
    	if(sb.length() > 0) {
    		return sb.substring(0, sb.length()-1);
    	}
    	return sb.toString();
    } 

	@Override
    public String toString() {
        return "TestTimeSequenceStatistics [testTime=" + testTime + ", maxTestTime=" + maxTestTime + ", errors="
                + errors + ", status1xx=" + status1xx + ", status2xx=" + status2xx + ", status3xx=" + status3xx
                + ", status4xx=" + status4xx + ", status5xx=" + status5xx + ", httpResponseLen=" + httpResponseLen
                + ", httpResponseErrors=" + httpResponseErrors + ", timeToResolveHost=" + timeToResolveHost
                + ", maxTimeToResolveHost=" + maxTimeToResolveHost + ", timeToEstabishConn=" + timeToEstabishConn
                + ", maxTimeToEstabishConn=" + maxTimeToEstabishConn + ", timeToFirstByte=" + timeToFirstByte
                + ", maxTimeToFirstByte=" + maxTimeToFirstByte  + ", requestCount=" + requestCount + "]";
    }
    
}
