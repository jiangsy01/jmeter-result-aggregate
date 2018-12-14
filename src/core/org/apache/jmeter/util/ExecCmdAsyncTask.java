
package org.apache.jmeter.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* @author jiangshaoyu
* @version createTime：2017年12月21日 
* decreption:
*/
public class ExecCmdAsyncTask implements Runnable {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExecCmdAsyncTask.class);
    String[] cmds;

    public ExecCmdAsyncTask(final String[] cmds) {
        this.cmds = cmds;
    }

    @Override
    public void run() {
        final Runtime runtime = Runtime.getRuntime();
        if (runtime == null) {
            // 指令没有执行，get(IS_SUCCESS)为null
            return;
        }
        final StringBuffer stdout = new StringBuffer(), stderr = new StringBuffer();
        BufferedReader stdoutReader = null, stderrReader = null;

        try {
            final Process process = runtime.exec(cmds);
            stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line;
            while ((line = stdoutReader.readLine()) != null) {
                stdout.append(line);
            }
            while ((line = stderrReader.readLine()) != null) {
                stderr.append(line);
            }

        } catch (final IOException e) {
            LOGGER.error("在本机执行指令时出错！ ", e);
        } finally {
            if (stdoutReader != null) {
                try {
                    stdoutReader.close();
                } catch (final IOException e) {
                    LOGGER.error("在本机执行指令时关闭流出错！ ", e);
                }
            }
            if (stderrReader != null) {
                try {
                    stderrReader.close();
                } catch (final IOException e) {
                    LOGGER.error("在本机执行指令时关闭流出错！ ", e);
                }
            }
        }

    }

}
