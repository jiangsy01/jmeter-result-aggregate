package org.apache.jmeter.reporters;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * Ptp Statistic Task.
 * 
 * @author jiangshaoyu
 */
public class PtpStatisticTask implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(PtpStatisticTask.class);
    private final Lock lock;
    
    private final CopyOnWriteArrayList<Long> longStatics;
    private final TestTimeSequenceStatistics testTimeSequenceStatistics;
    
    public PtpStatisticTask(final CopyOnWriteArrayList<Long> longStatics, final TestTimeSequenceStatistics testTimeSequenceStatistics, final Lock lock) {
        this.longStatics = longStatics;
        this.testTimeSequenceStatistics = testTimeSequenceStatistics;
        this.lock = lock;
    }

    @Override
    public void run() {
        try {
            lock.lockInterruptibly();
            testTimeSequenceStatistics.combine(longStatics);
        } catch (final InterruptedException e) {
            logger.error("get PtpStatisticTask lock fail", e);
        }finally {
            lock.unlock();
        }
    }
}
