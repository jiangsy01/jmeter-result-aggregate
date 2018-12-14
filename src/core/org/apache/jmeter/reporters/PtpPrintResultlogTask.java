package org.apache.jmeter.reporters;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.jmeter.reporters.ResultCollector.COMMA_DELIMITER;

/**
 * 
 * PtpPrintResultlogTask.
 * 
 * @author jiangshaoyu
 */
public class PtpPrintResultlogTask implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(PtpPrintResultlogTask.class);
    private final Lock lock;
    private static final AtomicLong startTimestamp = new AtomicLong(System.currentTimeMillis()/1000L);
//    private AtomicLong startTimestamp;
    private final Map<String, AtomicStampedReference<TestTimeSequenceStatistics>> m_testTimeSeqStatistics;
    private transient volatile PrintWriter m_agg_out;
    private final StringBuffer m_buffer_aggregate = new StringBuffer();
    public static AtomicInteger aggregateCount = new AtomicInteger(0);
    
//    public PtpPrintResultlogTask(final Map<String, AtomicStampedReference<TestTimeSequenceStatistics>> m_testTimeSeqStatistics, final PrintWriter agg_out, final Lock lock, AtomicLong startTimestamp) {
    public PtpPrintResultlogTask(final Map<String, AtomicStampedReference<TestTimeSequenceStatistics>> m_testTimeSeqStatistics, final PrintWriter agg_out, final Lock lock) {
        this.m_testTimeSeqStatistics = m_testTimeSeqStatistics;
        this.m_agg_out = agg_out;
        this.lock = lock;
//        this.startTimestamp = startTimestamp;
    }
    
    @Override
    public void run() {
        try {
            lock.lockInterruptibly();
            printResultLog(m_testTimeSeqStatistics);
        } catch (final InterruptedException e) {
            logger.error("get PtpPrintResultlogTask lock fail", e);
        } finally {
            lock.unlock();
        }
    }
    
    private synchronized void printResultLog(final Map<String, AtomicStampedReference<TestTimeSequenceStatistics>> m_testTimeSeqStatistics) {
        if(m_testTimeSeqStatistics == null || m_agg_out == null || m_testTimeSeqStatistics.size() < 1) {
            return;
        }
//        for(long printTime=startTimestamp.get(); printTime<(System.currentTimeMillis()/1000L); printTime=printTime+10) {
        for(long printTime=startTimestamp.get(); printTime<(System.currentTimeMillis()/1000L); printTime=printTime+1) {
            final Iterator<Entry<String, AtomicStampedReference<TestTimeSequenceStatistics>>> iterator = m_testTimeSeqStatistics.entrySet().iterator();
            while (iterator.hasNext()) {
                final Entry<String, AtomicStampedReference<TestTimeSequenceStatistics>> entry=iterator.next();  
                if(doPrintWork(entry, printTime)) {
                    iterator.remove();
                } else {
                    logger.error("printResultLog occurs a error! entry="+entry);
                    continue;
                }
            } 
        }
    }
    
    private synchronized boolean doPrintWork(final Entry<String, AtomicStampedReference<TestTimeSequenceStatistics>> entry, final long printTime) {
        if (entry == null || !entry.getKey().endsWith(String.valueOf(printTime)) || entry.getKey().split("\\|").length != 4 || entry.getValue() == null) {
            return false;
        }
        final String meta[] = entry.getKey().split("\\|");
        m_buffer_aggregate.append(meta[0]).append(COMMA_DELIMITER).append(meta[1]).append(COMMA_DELIMITER).append(meta[2]).append(COMMA_DELIMITER).append(meta[3]).append(COMMA_DELIMITER);
        m_buffer_aggregate.append(entry.getValue().getReference().getTestTime().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getMaxTestTime().get()).append(COMMA_DELIMITER)
                          .append(entry.getValue().getReference().getTestTimes()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getErrors().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getRequestCount().get()).append(COMMA_DELIMITER);
        if(entry.getValue().getReference().getHttpResponseLen().get() > 0) {
            m_buffer_aggregate.append(entry.getValue().getReference().getStatus1xx().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getStatus2xx().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getStatus3xx().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getStatus4xx().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getStatus5xx().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getHttpResponseLen().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getHttpResponseErrors().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getTimeToResolveHost().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getMaxTimeToResolveHost().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getTimeToEstabishConn().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getMaxTimeToEstabishConn().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getTimeToFirstByte().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getMaxTimeToFirstByte().get()).append(COMMA_DELIMITER).append(entry.getValue().getReference().getNewConnections().get());
        }
        m_agg_out.println(m_buffer_aggregate.toString());
        //for compare with org.apache.jmeter.reporters.ResultCollector.requestCount
        aggregateCount.addAndGet(entry.getValue().getReference().getRequestCount().get());
        //show thread pool status
        m_buffer_aggregate.delete(0, m_buffer_aggregate.length());
        return true;
    }

}
