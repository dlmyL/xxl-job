package com.xxl.job.core.log;

import com.xxl.job.core.biz.model.LogResult;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 该类是操作日志的类，对日志文件进行操作的功能全部封装在这里。
 * 执行器这一端不仅要把定时任务执行结果回调给调度中心，还要把定时任务执行
 * 的详细信息以日志的方式收集起来，并且输出到本地文件中。
 */
@Slf4j
public class XxlJobFileAppender {

    /**
     * 默认的日志存储路径，但是在执行器启动的时候，该路径会被用户在
     * 配置文件中设置的路径取代
     * strut like:
     * ---/
     * ---/gluesource/
     * ---/gluesource/10_1514171108000.js
     * ---/gluesource/10_1514171108000.js
     * ---/2017-12-25/
     * ---/2017-12-25/639.log
     * ---/2017-12-25/821.log
     */
    @Getter
    private static String logBasePath = "E:\\java\\SourceCode\\xxl-job\\data\\applogs\\xxl-job\\jobhandler";
    // 在Web端在线编辑代码，执行定时任务的时候，用这个路径把用户编辑的代码记录下来
    @Getter
    private static String glueSrcPath = logBasePath.concat("/gluesource");

    // 初始化存储日志文件路径
    public static void initLogPath(String logPath) {
        if (logPath != null && !logPath.trim().isEmpty()) {
            logBasePath = logPath;
        }

        File logPathDir = new File(logBasePath);
        if (!logPathDir.exists()) {
            logPathDir.mkdirs();
        }
        logBasePath = logPathDir.getPath();

        File glueBaseDir = new File(logPathDir, "gluesource");
        if (!glueBaseDir.exists()) {
            glueBaseDir.mkdirs();
        }
        glueSrcPath = glueBaseDir.getPath();
    }

    // 根据定时任务的触发时间和其对应的日志id创造一个文件名，这个日志id是在调度中心就创建好的
    // 通过触发器参数传递给执行器的
    public static String makeLogFileName(Date triggerDate, long logId) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        File logFilePath = new File(getLogBasePath(), sdf.format(triggerDate));
        if (!logFilePath.exists()) {
            logFilePath.mkdir();
        }

        String logFileName = logFilePath.getPath()
                .concat(File.separator)
                .concat(String.valueOf(logId))
                .concat(".log");

        return logFileName;
    }

    // 把日志记录到本地的日志文件中
    public static void appendLog(String logFileName, String appendLog) {
        if (logFileName == null || logFileName.trim().isEmpty()) {
            return;
        }
        File logFile = new File(logFileName);

        if (!logFile.exists()) {
            try {
                logFile.createNewFile();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                return;
            }
        }

        if (appendLog == null) {
            appendLog = "";
        }
        appendLog += "\r\n";

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(logFile, true);
            fos.write(appendLog.getBytes("utf-8"));
            fos.flush();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    // 读取本地的日志文件内容
    public static LogResult readLog(String logFileName, int fromLineNum) {
        if (logFileName == null || logFileName.trim().isEmpty()) {
            return new LogResult(fromLineNum, 0, "readLog fail, logFile not found", true);
        }

        File logFile = new File(logFileName);
        if (!logFile.exists()) {
            return new LogResult(fromLineNum, 0, "readLog fail, logFile not exists", true);
        }

        StringBuffer logContentBuffer = new StringBuffer();
        int toLineNum = 0;
        LineNumberReader reader = null;
        try {
            reader = new LineNumberReader(new InputStreamReader(new FileInputStream(logFile), StandardCharsets.UTF_8));
            String line = null;
            while ((line = reader.readLine()) != null) {
                toLineNum = reader.getLineNumber();
                if (toLineNum >= fromLineNum) {
                    logContentBuffer.append(line).append("\n");
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        return new LogResult(fromLineNum, toLineNum, logContentBuffer.toString(), false);
    }

    // 读取本地的日志文件内容，一行一行的读
    public static String readLines(File logFile) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "utf-8"));
            if (reader != null) {
                StringBuilder sb = new StringBuilder();
                String line = null;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append("\n");
                }
                return sb.toString();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        return null;
    }

}
