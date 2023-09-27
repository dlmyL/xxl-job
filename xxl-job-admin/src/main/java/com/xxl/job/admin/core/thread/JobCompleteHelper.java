package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.complete.XxlJobCompleter;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * <h1>调度中心接收执行器回调信息的工作组件</h1>
 */
public class JobCompleteHelper {

    private static Logger logger = LoggerFactory.getLogger(JobCompleteHelper.class);

    private static JobCompleteHelper instance = new JobCompleteHelper();

    public static JobCompleteHelper getInstance() {
        return instance;
    }

    // ---------------------- monitor ----------------------

    // 回调线程池，这个线程池就是处理执行器端回调过来的日志信息的
    private ThreadPoolExecutor callbackThreadPool = null;
    // 监控线程
    private Thread monitorThread;

    private volatile boolean toStop = false;

    public void start() {
        // 创建回调线程池
        callbackThreadPool = new ThreadPoolExecutor(
                2,
                20,
                30L,
                TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(3000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobLosedMonitorHelper-callbackThreadPool-" + r.hashCode());
                    }
                },
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        r.run();
                        logger.warn(">>>>>>>>>>> xxl-job, callback too fast, match threadpool rejected handler(run " +
								"now).");
                    }
                });


        // 创建监控线程池
        monitorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                // 这里休眠一会儿是因为需要等待 JobTriggerPoolHelper 组件初始化，因为不执行
                // 远程调度，也就没有回调过来的定时任务执行结果信息
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (InterruptedException e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                while (!toStop) {
                    try {
                        // 任务结果丢失处理：调度记录停留在 "运行中" 状态超过10min，且对应执行器心跳注册失败不在线，则将本地调度主动标记失败；
                        Date losedTime = DateUtil.addMinutes(new Date(), -10);
                        List<Long> losedJobIds =
								XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().findLostJobIds(losedTime);

                        if (losedJobIds != null && losedJobIds.size() > 0) {
                            for (Long logId : losedJobIds) {

                                XxlJobLog jobLog = new XxlJobLog();
                                jobLog.setId(logId);

                                jobLog.setHandleTime(new Date());
                                jobLog.setHandleCode(ReturnT.FAIL_CODE);
                                jobLog.setHandleMsg(I18nUtil.getString("joblog_lost_fail"));

                                XxlJobCompleter.updateHandleInfoAndFinish(jobLog);
                            }

                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(">>>>>>>>>>> xxl-job, job fail monitor thread error:{}", e);
                        }
                    }

                    try {
                        TimeUnit.SECONDS.sleep(60);
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }

                }

                logger.info(">>>>>>>>>>> xxl-job, JobLosedMonitorHelper stop");

            }
        });
        monitorThread.setDaemon(true);
        monitorThread.setName("xxl-job, admin JobLosedMonitorHelper");
        monitorThread.start();
    }

    public void toStop() {
        toStop = true;

        // stop registryOrRemoveThreadPool
        callbackThreadPool.shutdownNow();

        // stop monitorThread (interrupt and wait)
        monitorThread.interrupt();
        try {
            monitorThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }


    // ---------------------- helper ----------------------

    public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {

        callbackThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                for (HandleCallbackParam handleCallbackParam : callbackParamList) {
                    ReturnT<String> callbackResult = callback(handleCallbackParam);
                    logger.debug(">>>>>>>>> JobApiController.callback {}, handleCallbackParam={}, callbackResult={}",
                            (callbackResult.getCode() == ReturnT.SUCCESS_CODE ? "success" : "fail"), handleCallbackParam, callbackResult);
                }
            }
        });

        return ReturnT.SUCCESS;
    }

    private ReturnT<String> callback(HandleCallbackParam handleCallbackParam) {
        // valid log item
        XxlJobLog log = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().load(handleCallbackParam.getLogId());
        if (log == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "log item not found.");
        }
        if (log.getHandleCode() > 0) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "log repeate callback.");     // avoid repeat callback, trigger child job etc
        }

        // handle msg
        StringBuffer handleMsg = new StringBuffer();
        if (log.getHandleMsg() != null) {
            handleMsg.append(log.getHandleMsg()).append("<br>");
        }
        if (handleCallbackParam.getHandleMsg() != null) {
            handleMsg.append(handleCallbackParam.getHandleMsg());
        }

        // success, save log
        log.setHandleTime(new Date());
        log.setHandleCode(handleCallbackParam.getHandleCode());
        log.setHandleMsg(handleMsg.toString());
        XxlJobCompleter.updateHandleInfoAndFinish(log);

        return ReturnT.SUCCESS;
    }


}
