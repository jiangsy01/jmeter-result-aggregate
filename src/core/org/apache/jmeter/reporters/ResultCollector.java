/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jmeter.reporters;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.engine.util.NoThreadClone;
import org.apache.jmeter.gui.GuiPackage;
import org.apache.jmeter.samplers.Clearable;
import org.apache.jmeter.samplers.Remoteable;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleListener;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.samplers.SampleSaveConfiguration;
import org.apache.jmeter.save.CSVSaveService;
import org.apache.jmeter.save.SaveService;
import org.apache.jmeter.services.FileServer;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.testelement.property.BooleanProperty;
import org.apache.jmeter.testelement.property.ObjectProperty;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jmeter.util.R;
import org.apache.jmeter.visualizers.Visualizer;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.jorphan.util.JMeterError;
import org.apache.jorphan.util.JOrphanUtils;
import org.apache.log.Logger;

import org.apache.jmeter.util.ExecCmdOwn;

/**
 * This class handles all saving of samples.
 * The class must be thread-safe because it is shared between threads (NoThreadClone).
 */
public class ResultCollector extends AbstractListenerElement implements SampleListener, Clearable, Serializable,
        TestStateListener, Remoteable, NoThreadClone {

    private static final Logger log = LoggingManager.getLoggerForClass();

    private static final long serialVersionUID = 233L;

    // This string is used to identify local test runs, so must not be a valid host name
    private static final String TEST_IS_LOCAL = "*local*"; // $NON-NLS-1$

    private static final String TESTRESULTS_START = "<testResults>"; // $NON-NLS-1$

    private static final String TESTRESULTS_START_V1_1_PREVER = "<testResults version=\"";  // $NON-NLS-1$

    private static final String TESTRESULTS_START_V1_1_POSTVER="\">"; // $NON-NLS-1$

    private static final String TESTRESULTS_END = "</testResults>"; // $NON-NLS-1$

    // we have to use version 1.0, see bug 59973
    private static final String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"; // $NON-NLS-1$

    private static final int MIN_XML_FILE_LEN = XML_HEADER.length() + TESTRESULTS_START.length()
            + TESTRESULTS_END.length();

    public static final String FILENAME = "filename"; // $NON-NLS-1$
    
    public static final String AGG_FILENAME = "agg-filename";
    
    public static final String ROUNDID_PROP_NAME = "roundId";
    
    private static final String NETEASE_INVALID_VALUE = "-1";
    
    public static final String COMMA_DELIMITER = ",";
    
    public static final String VERTICAL_DELIMITER = "|";
    
    private static final String SN_COMMAND = "cat /etc/nagent_sn";
    
    private static final String SN_RESULT_KEY = "stdout";
    
    private static volatile String MACHINE_ID = "-1";
    
    private static volatile String ROUND_ID = "-1";
    
    private static final int JMETER_RESULT_ROW_SIZE = 8;
    
    public static final ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);
    
    public static final ConcurrentHashMap<String, AtomicStampedReference<TestTimeSequenceStatistics>> m_testTimeSeqStatistics = new ConcurrentHashMap<String, AtomicStampedReference<TestTimeSequenceStatistics>>(400);
    
    private static final Lock lock = new ReentrantLock();
    
    private static final AtomicInteger uploadLock = new AtomicInteger(0);
    
    public static AtomicInteger requestCount = new AtomicInteger(0);
    
    private static PtpPrintResultlogTask printResultlogTask;
    
    private static final long PTP_WAIT_AGGREGATE = 2000000000L;
    
    private static final boolean AGGREGATE_SAVING_AUTOFLUSH = true;
    
    private static final int AGGREGATE_FLUSH_SIZE = 2048;
    
//    private static final AtomicLong ROUND_START_TIMESTAMP = new AtomicLong((System.currentTimeMillis() / 10000L) * 10L);

    private static final String SAVE_CONFIG = "saveConfig"; // $NON-NLS-1$

    private static final String ERROR_LOGGING = "ResultCollector.error_logging"; // $NON-NLS-1$

    private static final String SUCCESS_ONLY_LOGGING = "ResultCollector.success_only_logging"; // $NON-NLS-1$

    /** AutoFlush on each line */
    private static final boolean SAVING_AUTOFLUSH = JMeterUtils.getPropDefault("jmeter.save.saveservice.autoflush", false); //$NON-NLS-1$

    // Static variables

    // Lock used to guard static mutable variables
    private static final Object LOCK = new Object();

    //@GuardedBy("LOCK")
    private static final Map<String, FileEntry> files = new HashMap<>();

    /**
     * Shutdown Hook that ensures PrintWriter is flushed is CTRL+C or kill is called during a test
     */
    //@GuardedBy("LOCK")
    private static Thread shutdownHook;

    /*
     * Keep track of the file writer and the configuration,
     * as the instance used to close them is not the same as the instance that creates
     * them. This means one cannot use the saved PrintWriter or use getSaveConfig()
     */
    private static class FileEntry{
        final PrintWriter pw;
        final SampleSaveConfiguration config;
        FileEntry(PrintWriter _pw, SampleSaveConfiguration _config){
            pw =_pw;
            config = _config;
        }
    }

    /**
     * The instance count is used to keep track of whether any tests are currently running.
     * It's not possible to use the constructor or threadStarted etc as tests may overlap
     * e.g. a remote test may be started,
     * and then a local test started whilst the remote test is still running.
     */
    //@GuardedBy("LOCK")
    private static int instanceCount; // Keep track of how many instances are active

    // Instance variables (guarded by volatile)

    private transient volatile PrintWriter out;
    
    private transient volatile PrintWriter agg_out;

    private volatile boolean inTest = false;

    private volatile boolean isStats = false;

    /** the summarizer to which this result collector will forward the samples */
    private volatile Summariser summariser;

    private static final class ShutdownHook implements Runnable {

        @Override
        public void run() {
            log.info("Shutdown hook started");
            synchronized (LOCK) {
                flushFileOutput();                    
            }
            log.info("Shutdown hook ended");
        }     
    }
    
    /**
     * No-arg constructor.
     */
    public ResultCollector() {
        this(null);
    }

    /**
     * Constructor which sets the used {@link Summariser}
     * @param summer The {@link Summariser} to use
     */
    public ResultCollector(Summariser summer) {
        setErrorLogging(false);
        setSuccessOnlyLogging(false);
        setProperty(new ObjectProperty(SAVE_CONFIG, new SampleSaveConfiguration()));
        summariser = summer;
    }

    // Ensure that the sample save config is not shared between copied nodes
    // N.B. clone only seems to be used for client-server tests
    @Override
    public Object clone(){
        ResultCollector clone = (ResultCollector) super.clone();
        clone.setSaveConfig((SampleSaveConfiguration)clone.getSaveConfig().clone());
        // Unfortunately AbstractTestElement does not call super.clone()
        clone.summariser = this.summariser;
        return clone;
    }

    private void setFilenameProperty(String f) {
        setProperty(FILENAME, f);
    }
    
    private void setAggFilenameProperty(String f) {
        setProperty(AGG_FILENAME, f);
    }

    /**
     * Get the filename of the file this collector uses
     * 
     * @return The name of the file
     */
    public String getFilename() {
        return getPropertyAsString(FILENAME);
    }
    
    public String getAggFilename() {
        return getPropertyAsString(AGG_FILENAME);
    }

    /**
     * Get the state of error logging
     * 
     * @return Flag whether errors should be logged
     */
    public boolean isErrorLogging() {
        return getPropertyAsBoolean(ERROR_LOGGING);
    }

    /**
     * Sets error logging flag
     * 
     * @param errorLogging
     *            The flag whether errors should be logged
     */
    public final void setErrorLogging(boolean errorLogging) {
        setProperty(new BooleanProperty(ERROR_LOGGING, errorLogging));
    }

    /**
     * Sets the flag whether only successful samples should be logged
     * 
     * @param value
     *            The flag whether only successful samples should be logged
     */
    public final void setSuccessOnlyLogging(boolean value) {
        if (value) {
            setProperty(new BooleanProperty(SUCCESS_ONLY_LOGGING, true));
        } else {
            removeProperty(SUCCESS_ONLY_LOGGING);
        }
    }

    /**
     * Get the state of successful only logging
     * 
     * @return Flag whether only successful samples should be logged
     */
    public boolean isSuccessOnlyLogging() {
        return getPropertyAsBoolean(SUCCESS_ONLY_LOGGING,false);
    }

    /**
     * Decides whether or not to a sample is wanted based on:
     * <ul>
     * <li>errorOnly</li>
     * <li>successOnly</li>
     * <li>sample success</li>
     * </ul>
     * Should only be called for single samples.
     *
     * @param success is sample successful
     * @return whether to log/display the sample
     */
    public boolean isSampleWanted(boolean success){
        boolean errorOnly = isErrorLogging();
        boolean successOnly = isSuccessOnlyLogging();
        return isSampleWanted(success, errorOnly, successOnly);
    }

    /**
     * Decides whether or not to a sample is wanted based on:
     * <ul>
     * <li>errorOnly</li>
     * <li>successOnly</li>
     * <li>sample success</li>
     * </ul>
     * This version is intended to be called by code that loops over many samples;
     * it is cheaper than fetching the settings each time.
     * @param success status of sample
     * @param errorOnly if errors only wanted
     * @param successOnly if success only wanted
     * @return whether to log/display the sample
     */
    public static boolean isSampleWanted(boolean success, boolean errorOnly,
            boolean successOnly) {
        return (!errorOnly && !successOnly) ||
               (success && successOnly) ||
               (!success && errorOnly);
        // successOnly and errorOnly cannot both be set
    }
    /**
     * Sets the filename attribute of the ResultCollector object.
     *
     * @param f
     *            the new filename value
     */
    public void setFilename(String f) {
        if (inTest) {
            return;
        }
        setFilenameProperty(f);
    }
    
    public void setAggFilename(String f) {
        if (inTest) {
            return;
        }
        setAggFilenameProperty(f);
    }

    @Override
    public void testEnded(String host) {
        synchronized(LOCK){
            instanceCount--;
            if (instanceCount <= 0) {
                // No need for the hook now
                // Bug 57088 - prevent (im?)possible NPE
                if (shutdownHook != null) {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } else {
                    log.warn("Should not happen: shutdownHook==null, instanceCount=" + instanceCount);
                }
                finalizeFileOutput();
                inTest = false;
            }
        }

        if(summariser != null) {
            summariser.testEnded(host);
        }
        ptpClear();
    }

    @Override
    public void testStarted(String host) {
        synchronized(LOCK){
            if (instanceCount == 0) { // Only add the hook once
                shutdownHook = new Thread(new ShutdownHook());
                Runtime.getRuntime().addShutdownHook(shutdownHook);
            }
            instanceCount++;
            try {
                initializeFileOutput();
                aggInitializeFileOutput();
                if (getVisualizer() != null) {
                    this.isStats = getVisualizer().isStats();
                }
            } catch (Exception e) {
                log.error("", e);
            }
        }
        inTest = true;

        if(summariser != null) {
            summariser.testStarted(host);
        }
    }

    @Override
    public void testEnded() {
        testEnded(TEST_IS_LOCAL);
    }

    @Override
    public void testStarted() {
        testStarted(TEST_IS_LOCAL);
    }

    /**
     * Loads an existing sample data (JTL) file.
     * This can be one of:
     * <ul>
     *   <li>XStream format</li>
     *   <li>CSV format</li>
     * </ul>
     *
     */
    public void loadExistingFile() {
        final Visualizer visualizer = getVisualizer();
        if (visualizer == null) {
            return; // No point reading the file if there's no visualiser
        }
        boolean parsedOK = false;
        String filename = getFilename();
        File file = new File(filename);
        if (file.exists()) {
            BufferedReader dataReader = null;
            BufferedInputStream bufferedInputStream = null;
            try {
                dataReader = new BufferedReader(new FileReader(file)); // TODO Charset ?
                // Get the first line, and see if it is XML
                String line = dataReader.readLine();
                dataReader.close();
                dataReader = null;
                if (line == null) {
                    log.warn(filename+" is empty");
                } else {
                    if (!line.startsWith("<?xml ")){// No, must be CSV //$NON-NLS-1$
                        CSVSaveService.processSamples(filename, visualizer, this);
                        parsedOK = true;
                    } else { // We are processing XML
                        try { // Assume XStream
                            bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
                            SaveService.loadTestResults(bufferedInputStream,
                                    new ResultCollectorHelper(this, visualizer));
                            parsedOK = true;
                        } catch (Exception e) {
                            log.warn("Failed to load " + filename + " using XStream. Error was: " + e);
                        }
                    }
                }
            } catch (IOException | JMeterError | RuntimeException | OutOfMemoryError e) {
                // FIXME Why do we catch OOM ?
                log.warn("Problem reading JTL file: " + file);
            } finally {
                JOrphanUtils.closeQuietly(dataReader);
                JOrphanUtils.closeQuietly(bufferedInputStream);
                if (!parsedOK) {
                    GuiPackage.showErrorMessage(
                                "Error loading results file - see log file",
                                "Result file loader");
                }
            }
        } else {
            GuiPackage.showErrorMessage(
                    "Error loading results file - could not open file",
                    "Result file loader");
        }
    }

    private static void writeFileStart(PrintWriter writer, SampleSaveConfiguration saveConfig) {
        if (saveConfig.saveAsXml()) {
            writer.print(XML_HEADER);
            // Write the EOL separately so we generate LF line ends on Unix and Windows
            writer.print("\n"); // $NON-NLS-1$
            String pi=saveConfig.getXmlPi();
            if (pi.length() > 0) {
                writer.println(pi);
            }
            // Can't do it as a static initialisation, because SaveService
            // is being constructed when this is called
            writer.print(TESTRESULTS_START_V1_1_PREVER);
            writer.print(SaveService.getVERSION());
            writer.print(TESTRESULTS_START_V1_1_POSTVER);
            // Write the EOL separately so we generate LF line ends on Unix and Windows
            writer.print("\n"); // $NON-NLS-1$
        } else if (saveConfig.saveFieldNames()) {
            writer.println(CSVSaveService.printableFieldNamesToString(saveConfig));
        }
    }

    private static void writeFileEnd(PrintWriter pw, SampleSaveConfiguration saveConfig) {
        if (saveConfig.saveAsXml()) {
            pw.print("\n"); // $NON-NLS-1$
            pw.print(TESTRESULTS_END);
            pw.print("\n");// Added in version 1.1 // $NON-NLS-1$
        }
    }

    private static PrintWriter getFileWriter(String filename, SampleSaveConfiguration saveConfig)
            throws IOException {
        if (filename == null || filename.length() == 0) {
            return null;
        }
        filename = FileServer.resolveBaseRelativeName(filename);
        FileEntry fe = files.get(filename);
        PrintWriter writer = null;
        boolean trimmed = true;

        if (fe == null) {
            if (saveConfig.saveAsXml()) {
                trimmed = trimLastLine(filename);
            } else {
                trimmed = new File(filename).exists();
            }
            // Find the name of the directory containing the file
            // and create it - if there is one
            File pdir = new File(filename).getParentFile();
            if (pdir != null) {
                // returns false if directory already exists, so need to check again
                if(pdir.mkdirs()){
                    log.info("Folder "+pdir.getAbsolutePath()+" was created");
                } // else if might have been created by another process so not a problem
                if (!pdir.exists()){
                    log.warn("Error creating directories for "+pdir.toString());
                }
            }
            writer = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(filename,
                    trimmed)), SaveService.getFileEncoding(StandardCharsets.UTF_8.name())), SAVING_AUTOFLUSH);
            log.debug("Opened file: "+filename);
            files.put(filename, new FileEntry(writer, saveConfig));
        } else {
            writer = fe.pw;
        }
        if (!trimmed) {
            writeFileStart(writer, saveConfig);
        }
        return writer;
    }
    
    private static PrintWriter getAggFileWriter(String filename, SampleSaveConfiguration saveConfig)
            throws IOException {
        if (filename == null || filename.length() == 0) {
            return null;
        }
        filename = FileServer.resolveBaseRelativeName(filename);
        FileEntry fe = files.get(filename);
        PrintWriter writer = null;
        boolean trimmed = true;

        if (fe == null) {
            if (saveConfig.saveAsXml()) {
                trimmed = trimLastLine(filename);
            } else {
                trimmed = new File(filename).exists();
            }
            // Find the name of the directory containing the file
            // and create it - if there is one
            File pdir = new File(filename).getParentFile();
            if (pdir != null) {
                // returns false if directory already exists, so need to check again
                if(pdir.mkdirs()){
                    log.info("Folder "+pdir.getAbsolutePath()+" was created");
                } // else if might have been created by another process so not a problem
                if (!pdir.exists()){
                    log.warn("Error creating directories for "+pdir.toString());
                }
            }
            writer = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(filename,
                    trimmed), AGGREGATE_FLUSH_SIZE), SaveService.getFileEncoding(StandardCharsets.UTF_8.name())), AGGREGATE_SAVING_AUTOFLUSH);
//            writer = new PrintWriter(new OutputStreamWriter(new FilterOutputStream(new FileOutputStream(filename,
//                  trimmed)), SaveService.getFileEncoding(StandardCharsets.UTF_8.name())), AGGREGATE_SAVING_AUTOFLUSH);
            log.debug("Opened file: "+filename);
            files.put(filename, new FileEntry(writer, saveConfig));
        } else {
            writer = fe.pw;
        }
        if (!trimmed) {
            writeFileStart(writer, saveConfig);
        }
        return writer;
    }

    // returns false if the file did not contain the terminator
    private static boolean trimLastLine(String filename) {
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(filename, "rw"); // $NON-NLS-1$
            long len = raf.length();
            if (len < MIN_XML_FILE_LEN) {
                return false;
            }
            raf.seek(len - TESTRESULTS_END.length() - 10);// TODO: may not work on all OSes?
            String line;
            long pos = raf.getFilePointer();
            int end = 0;
            while ((line = raf.readLine()) != null)// reads to end of line OR end of file
            {
                end = line.indexOf(TESTRESULTS_END);
                if (end >= 0) // found the string
                {
                    break;
                }
                pos = raf.getFilePointer();
            }
            if (line == null) {
                log.warn("Unexpected EOF trying to find XML end marker in " + filename);
                raf.close();
                return false;
            }
            raf.setLength(pos + end);// Truncate the file
            raf.close();
            raf = null;
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            log.warn("Error trying to find XML terminator " + e.toString());
            return false;
        } finally {
            try {
                if (raf != null) {
                    raf.close();
                }
            } catch (IOException e1) {
                log.info("Could not close " + filename + " " + e1.getLocalizedMessage());
            }
        }
        return true;
    }

    @Override
    public void sampleStarted(SampleEvent e) {
    }

    @Override
    public void sampleStopped(SampleEvent e) {
    }

    /**
     * When a test result is received, display it and save it.
     *
     * @param event
     *            the sample event that was received
     */
    @Override
    public void sampleOccurred(SampleEvent event) {
        SampleResult result = event.getResult();

        if (isSampleWanted(result.isSuccessful())) {
            sendToVisualizer(result);
            if (out != null && !isResultMarked(result) && !this.isStats) {
                SampleSaveConfiguration config = getSaveConfig();
                result.setSaveConfig(config);
                try {
                    if (config.saveAsXml()) {
                        SaveService.saveSampleResult(event, out);
                        if (agg_out!=null && !aggIsResultMarked(result)) {
                            SaveService.saveSampleResult(event, agg_out);
                        }
                    } else { // !saveAsXml
                        String savee = CSVSaveService.resultToDelimitedString(event);
                        out.println(savee);
                        requestCount.incrementAndGet();
                        printAggregateResult(savee);
                    }
                } catch (Exception err) {
                    log.error("Error trying to record a sample", err); // should throw exception back to caller
                }
            }
        }

        if(summariser != null) {
            summariser.sampleOccurred(event);
        }
    }

    protected final void sendToVisualizer(SampleResult r) {
        if (getVisualizer() != null) {
            getVisualizer().add(r);
        }
    }

    /**
     * recordStats is used to save statistics generated by visualizers
     *
     * @param e The data to save
     * @throws IOException when data writing fails
     */
    // Used by: MonitorHealthVisualizer.add(SampleResult res)
    public void recordStats(TestElement e) throws IOException {
        if (out != null) {
            SaveService.saveTestElement(e, out);
        }
    }
    
    public void aggRecordStats(TestElement e) throws IOException {
        if (agg_out != null) {
            SaveService.saveTestElement(e, agg_out);
        }
    }

    /**
     * Checks if the sample result is marked or not, and marks it
     * @param res - the sample result to check
     * @return <code>true</code> if the result was marked
     */
    private boolean isResultMarked(SampleResult res) {
        String filename = getFilename();
        return res.markFile(filename);
    }
    
    private boolean aggIsResultMarked(SampleResult res) {
        String filename = getAggFilename();
        return res.markFile(filename);
    }

    private void initializeFileOutput() throws IOException {

        String filename = getFilename();
        if (filename != null) {
            if (out == null) {
                try {
                    out = getFileWriter(filename, getSaveConfig());
                } catch (FileNotFoundException e) {
                    out = null;
                }
            }
        }
    }
    
    private void aggInitializeFileOutput() throws IOException {
        String filename = getAggFilename();
        if(filename != null) {
            if(agg_out == null) {
                try {
                    agg_out = getAggFileWriter(filename, getSaveConfig());
                    if (agg_out!=null) {
                        printResultLogs(m_testTimeSeqStatistics);
                    }
                } catch (Exception e) {
                    agg_out = null;
                }
            }
        }
    }

    /**
     * Flush PrintWriter to synchronize file contents
     */
    public void flushFile() {
        if (out != null) {
            log.info("forced flush through ResultCollector#flushFile");
            out.flush();
        }
    }
    
    public void aggFlushFile() {
        if(agg_out != null) {
            log.info("aggregate forced flush through ResultCollector#flushFile");
            agg_out.flush();
        }
    }

    /**
     * Flush PrintWriter, called by Shutdown Hook to ensure no data is lost
     */
    private static void flushFileOutput() {
        for(Map.Entry<String,ResultCollector.FileEntry> me : files.entrySet()){
            log.debug("Flushing: "+me.getKey());
            FileEntry fe = me.getValue();
            fe.pw.flush();
            if (fe.pw.checkError()){
                log.warn("Problem detected during use of "+me.getKey());
            }
        }
    }
    
    private void finalizeFileOutput() {
        for(Map.Entry<String,ResultCollector.FileEntry> me : files.entrySet()){
            log.debug("Closing: "+me.getKey());
            FileEntry fe = me.getValue();
            writeFileEnd(fe.pw, fe.config);
            fe.pw.close();
            if (fe.pw.checkError()){
                log.warn("Problem detected during use of "+me.getKey());
            }
        }
        files.clear();
    }

    /**
     * @return Returns the saveConfig.
     */
    public SampleSaveConfiguration getSaveConfig() {
        try {
            return (SampleSaveConfiguration) getProperty(SAVE_CONFIG).getObjectValue();
        } catch (ClassCastException e) {
            setSaveConfig(new SampleSaveConfiguration());
            return getSaveConfig();
        }
    }

    /**
     * @param saveConfig
     *            The saveConfig to set.
     */
    public void setSaveConfig(SampleSaveConfiguration saveConfig) {
        getProperty(SAVE_CONFIG).setObjectValue(saveConfig);
    }

    // This is required so that
    // @see org.apache.jmeter.gui.tree.JMeterTreeModel.getNodesOfType()
    // can find the Clearable nodes - the userObject has to implement the interface.
    @Override
    public void clearData() {
    }
    
    private void printAggregateResult(String savee) {
        if (agg_out == null || savee == null || savee.length() <= 0 || savee.split(COMMA_DELIMITER).length != JMETER_RESULT_ROW_SIZE) {
            return ;
        }
        if (ROUND_ID == null || ROUND_ID.equals(NETEASE_INVALID_VALUE)) {
            ROUND_ID = getPropertyAsString(ROUNDID_PROP_NAME);
        }
        if (MACHINE_ID == null || MACHINE_ID.equals(NETEASE_INVALID_VALUE)) {
            MACHINE_ID = getMachineSnByCMD();
        }
        String testId = getTestIdFromSavee(savee);
        String timestamp = savee.substring(0, savee.indexOf(COMMA_DELIMITER,0));
//        String tenSecondTimestamp = String.valueOf((Long.parseLong(timestamp)/10000L)*10L);
        if (ROUND_ID!=null && MACHINE_ID!=null && testId!=null) {
            final String keyName = ROUND_ID+VERTICAL_DELIMITER+MACHINE_ID+VERTICAL_DELIMITER+testId+VERTICAL_DELIMITER+Long.parseLong(timestamp)/1000L;
            aggregateTimeSequence(keyName, parseEveryResponseRow(savee, timestamp));
//            final String keyName = ROUND_ID+VERTICAL_DELIMITER+MACHINE_ID+VERTICAL_DELIMITER+testId+VERTICAL_DELIMITER+tenSecondTimestamp;
//            aggregateTimeSequence(keyName, parseEveryResponseRow(savee, tenSecondTimestamp));
        }
    }
    
    private String getMachineSnByCMD() {
        try {
            R result = ExecCmdOwn.execCmdReturnString(SN_COMMAND);
            return ( result == null || result.get(SN_RESULT_KEY) == null || result.get(SN_RESULT_KEY).equals("") )  ? NETEASE_INVALID_VALUE : (String)result.get(SN_RESULT_KEY);
        } catch (Exception e) {
            return NETEASE_INVALID_VALUE;
        } 
    }
    
    private String getTestIdFromSavee(String savee) {
        if (savee == null) {
            return null;
        }
        return savee.substring(savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,1)+1)+1, savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,1)+1)+1));
    }
    
    private CopyOnWriteArrayList<Long> parseEveryResponseRow(String savee, String timestamp) {
        CopyOnWriteArrayList<Long> longStatics = new CopyOnWriteArrayList<Long>();
        
        if (timestamp!=null) {
            longStatics.add(Long.parseLong(timestamp.trim()));
        }
        String elapsed = savee.substring(savee.indexOf(COMMA_DELIMITER,0)+1, savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,0)+1));
        if (elapsed!=null) {
            longStatics.add(Long.parseLong(elapsed.trim()));
        }
        String statusCode = savee.substring(savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,1)+1)+1)+1, savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,1)+1)+1)+1));
        if(statusCode!=null) {
            if (StringUtils.isNumeric(statusCode.trim())) {
                longStatics.add(Long.parseLong(statusCode.trim()));
            }else {
                longStatics.add(504L);
            }
        }
        String successCode = savee.substring(savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,1)+1)+1)+1)+1, savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,1)+1)+1)+1)+1));
        if (successCode!=null) {
            longStatics.add(Boolean.parseBoolean(successCode.trim()) ? 0L : 1L);
        }
        String bytes = savee.substring(savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,1)+1)+1)+1)+1)+1, savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,1)+1)+1)+1)+1)+1));
        if (bytes!=null) {
            longStatics.add(Long.parseLong(bytes.trim()));
        }
        String latency = savee.substring(savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,1)+1)+1)+1)+1)+1)+1, savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,1)+1)+1)+1)+1)+1)+1));
        if (latency!=null) {
            longStatics.add(Long.parseLong(latency.trim()));
        }
        String connect = savee.substring(savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER, savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,savee.indexOf(COMMA_DELIMITER,1)+1)+1)+1)+1)+1)+1)+1);
        if(connect!=null) {
            longStatics.add(Long.parseLong(connect.trim()));
        }
//        System.out.println("-----------longStatics:" + longStatics);
        return longStatics;
    }
    
    private void aggregateTimeSequence(final String keyName, final CopyOnWriteArrayList<Long> longStatics) {
        try {
            lock.lockInterruptibly();
            if (longStatics == null) {
                return;
            }
            if(!m_testTimeSeqStatistics.containsKey(keyName)) {
                m_testTimeSeqStatistics.put(keyName, new AtomicStampedReference<TestTimeSequenceStatistics>(new TestTimeSequenceStatistics(longStatics), 0));
                return;
            }
            timeSequenceStatisticsWork(longStatics, m_testTimeSeqStatistics.get(keyName).getReference());
        } catch (Exception e) {
            log.error("ptp aggregate occur exception:"+e);
        } finally {
            lock.unlock();
        }
    }
    
    private synchronized void timeSequenceStatisticsWork(final CopyOnWriteArrayList<Long> longStaticsArray, final TestTimeSequenceStatistics testTimeSequenceStatistics) {
        if(testTimeSequenceStatistics==null || longStaticsArray.size() < 2) {
            return;
        }
        final PtpStatisticTask statisticTask = new PtpStatisticTask(longStaticsArray, testTimeSequenceStatistics, lock);
        exec.execute(statisticTask); 
    }
    
    private synchronized void printResultLogs(final Map<String, AtomicStampedReference<TestTimeSequenceStatistics>> m_testTimeSeqStatistics) {
        if(uploadLock.get() == 0) {
            printResultlogTask = new PtpPrintResultlogTask(m_testTimeSeqStatistics, agg_out, lock);
            exec.scheduleAtFixedRate(printResultlogTask, 3, 1, TimeUnit.SECONDS);
//            printResultlogTask = new PtpPrintResultlogTask(m_testTimeSeqStatistics, agg_out, lock, ROUND_START_TIMESTAMP);
//            exec.scheduleAtFixedRate(printResultlogTask, 12, 10, TimeUnit.SECONDS);
            uploadLock.incrementAndGet();
        }
    }
    
    public synchronized void lastPrintResultLogTask() {
        final PtpPrintResultlogTask printResultlogTask = new PtpPrintResultlogTask(m_testTimeSeqStatistics, agg_out, lock);
//        final PtpPrintResultlogTask printResultlogTask = new PtpPrintResultlogTask(m_testTimeSeqStatistics, agg_out, lock, ROUND_START_TIMESTAMP);
        printResultlogTask.run();
    }
    
    private void ptpClear() {
        LockSupport.parkNanos(PTP_WAIT_AGGREGATE);
        if (m_testTimeSeqStatistics != null && m_testTimeSeqStatistics.size() > 0) {
            lastPrintResultLogTask();
        }
        System.out.println("ptp----------total request: "+ResultCollector.requestCount.get());
        System.out.println("ptp----------aggregate total request: "+PtpPrintResultlogTask.aggregateCount.get());
        if (agg_out != null) {
            agg_out.flush();
            agg_out.close();
        }
        if (exec != null) {
            exec.shutdown();
        }
    }
}
