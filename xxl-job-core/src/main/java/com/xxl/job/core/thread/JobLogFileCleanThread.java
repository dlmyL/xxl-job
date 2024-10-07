package com.xxl.job.core.thread;

import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.util.FileUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Job日志清除线程
 */
@Slf4j
public class JobLogFileCleanThread {

    @Getter
    private static JobLogFileCleanThread instance = new JobLogFileCleanThread();

    // 日志清除工作线程
    private Thread localThread;
    private volatile boolean toStop = false;

    public void start(final long logRetentionDays) {
        /*
        logRetentionDays为用户在配置文件设定的日志过期时间，默认
        是7天，这里有个判断，如果日志过期时间少于3天就直接退出。
         */
        if (logRetentionDays < 3) {
            return;
        }

        /*
        得到创建的所有日志文件夹，日志文件夹的名称就是该文件夹创建的时间。
        如果当前时间减去文件夹创建的时间大于用户设定的过期时间了，说明该
        文件夹中存储的日志已经过期了，可以被删除了，然后删除即可。
         */
        localThread = new Thread(() -> {
            while (!toStop) {
                try {
                    // 得到该路径下的所有日志文件，默认是/data/applogs/xxl-job/jobhandler文件夹
                    File[] childDirs = new File(XxlJobFileAppender.getLogBasePath()).listFiles();
                    // 判断日志文件数组非空
                    if (childDirs != null && childDirs.length > 0) {
                        // 得到当前时间
                        Calendar todayCal = Calendar.getInstance();
                        todayCal.set(Calendar.HOUR_OF_DAY, 0);
                        todayCal.set(Calendar.MINUTE, 0);
                        todayCal.set(Calendar.SECOND, 0);
                        todayCal.set(Calendar.MILLISECOND, 0);

                        Date todayDate = todayCal.getTime();

                        // 遍历日志文件
                        for (File childFile : childDirs) {
                            /*
                            如果不是文件夹就跳过这次循环，因为现在找到的都是文件夹，文件夹的名称是定时任务
                            执行的年月日时间，比如，2023-09-05，2023-10-02等等，每个时间都是一个文件夹，
                            文件夹中有很多个日志文件，文件名称就是定时任务的ID。
                             */
                            if (!childFile.isDirectory()) {
                                continue;
                            }

                            // 判断文件夹中是否有-符号，如果没有则跳过这个文件夹
                            if (!childFile.getName().contains("-")) {
                                continue;
                            }

                            // 该变量就用来记录日志文件的创建时间，其实就是文件夹的名字
                            Date logFileCreateDate = null;
                            try {
                                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                // 得到创建时间
                                logFileCreateDate = simpleDateFormat.parse(childFile.getName());
                            } catch (ParseException e) {
                                log.error(e.getMessage(), e);
                            }

                            if (logFileCreateDate == null) {
                                continue;
                            }

                            /*
                            计算刚才得到的今天的零点时间减去日志文件创建的时间是否大于了用户
                            设定的日志过期时间，如果超过了就把过期的日志删除了。
                             */
                            if ((todayDate.getTime() - logFileCreateDate.getTime()) >= (logRetentionDays * (24 * 60 * 60 * 1000))) {
                                FileUtil.deleteRecursively(childFile);
                            }
                        }

                    }
                } catch (Exception e) {
                    if (!toStop) {
                        log.error(e.getMessage(), e);
                    }
                }

                try {
                    TimeUnit.DAYS.sleep(1);
                } catch (InterruptedException e) {
                    if (!toStop) {
                        log.error(e.getMessage(), e);
                    }
                }

            }

            log.info(">>>>>>>>>>> xxl-job, executor JobLogFileCleanThread thread destroy.");
        });

        localThread.setDaemon(true);
        localThread.setName("xxl-job, executor JobLogFileCleanThread");
        localThread.start();
    }

    public void toStop() {
        toStop = true;
        if (localThread == null) {
            return;
        }
        localThread.interrupt();
        try {
            localThread.join();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }
}
