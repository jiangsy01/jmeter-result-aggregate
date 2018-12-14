package org.apache.jmeter.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**指令没有执行，get(IS_SUCCESS)为null；指令执行了，且成功了，get(IS_SUCCESS)为true；指令执行了，但失败了，get(IS_SUCCESS)为false
* @author jiangshaoyu
* @version createTime：2017年12月14日 
* decreption:
*/
public final class ExecCmdOwn {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExecCmdOwn.class);
    
    // 本机执行linux命令，执行结果标准输出
    private static final String EXECUTE_COMMAND_STDOUT = "stdout";
    // 本机执行linux命令，执行结果错误输出
    private static final String EXECUTE_COMMAND_STDERR = "stderr";
    private static final String IS_SUCCESS = "isSuccess";
    
    private static final ExecutorService exec = Executors.newSingleThreadExecutor();

    public static R execCmdReturnStringAsync(final String cmd) {
        final String[] cmds = new String[] { "/bin/sh", "-c", cmd };
        final ExecCmdAsyncTask task = new ExecCmdAsyncTask(cmds);
        try {
            exec.execute(task);
        } catch (final Exception e) {
            LOGGER.error("在本机执行指令时出错！ ");
            return R.ok().put(IS_SUCCESS, false);
        }
        return R.ok().put(IS_SUCCESS, true);
    }

    public static R execCmdReturnList(final String cmd) {
        final String[] cmds = new String[] { "/bin/sh", "-c", cmd };
        boolean result = false;
        final Runtime runtime = Runtime.getRuntime();
        if (runtime == null) {
            //指令没有执行，get(IS_SUCCESS)为null
            return R.ok();
        } 
        final List<String> stdout = new ArrayList<String>(), stderr = new ArrayList<String>();
        BufferedReader stdoutReader = null, stderrReader = null;
        
        try {
            final Process process = runtime.exec(cmds);
            stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line;
            while ((line = stdoutReader.readLine()) != null) {
                stdout.add(line);
            }
            while ((line = stderrReader.readLine()) != null) {
                stderr.add(line);
            }
            result = true;
            
        } catch (final IOException e) {
            LOGGER.error("在本机执行指令时出错！ ");
        } finally {
            if (stdoutReader != null) {
                try {
                    stdoutReader.close();
                } catch (final IOException e) {
                    LOGGER.error("在本机执行指令时关闭流出错！ ");
                }
            }
            if(stderrReader != null) {
                try {
                    stderrReader.close();
                } catch (final IOException e) {
                    LOGGER.error("在本机执行指令时关闭流出错！ ");
                }
            }
        }

        if(result) {
            //指令执行了，且成功了，get(IS_SUCCESS)为true
            return R.ok().put(IS_SUCCESS, result).put(EXECUTE_COMMAND_STDOUT, stdout).put(EXECUTE_COMMAND_STDERR, stderr);
        }
        //指令执行了，但失败了，get(IS_SUCCESS)为false
        return R.ok().put(IS_SUCCESS, result);
    }
    
    public boolean judgeExecResult(final Map<String, Object> result) {
        if (result != null && result.get(IS_SUCCESS) != null && (result.get(IS_SUCCESS) instanceof Boolean ) && ((Boolean)result.get(IS_SUCCESS)).equals(true)) {
            return true;
        }
        return false;
    }
    
    public static R execCmdReturnString(final String cmd) {
        final String[] cmds = new String[] { "/bin/sh", "-c", cmd };
        boolean result = false;
        final Runtime runtime = Runtime.getRuntime();
        if (runtime == null) {
            //指令没有执行，get(IS_SUCCESS)为null
            return R.ok();
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
            result = true;
            
        } catch (final IOException e) {
            LOGGER.error("在本机执行指令时出错！ ");
        } finally {
            if (stdoutReader != null) {
                try {
                    stdoutReader.close();
                } catch (final IOException e) {
                    LOGGER.error("在本机执行指令时关闭流出错！ ");
                }
            }
            if(stderrReader != null) {
                try {
                    stderrReader.close();
                } catch (final IOException e) {
                    LOGGER.error("在本机执行指令时关闭流出错！ ");
                }
            }
        }
        if(result) {
            //指令执行了，且成功了，get(IS_SUCCESS)为true
            return R.ok().put(IS_SUCCESS, result).put(EXECUTE_COMMAND_STDOUT, stdout.toString()).put(EXECUTE_COMMAND_STDERR, stderr.toString());
        }
        //指令执行了，但失败了，get(IS_SUCCESS)为false
        return R.ok().put(IS_SUCCESS, result);
    }
    

}
