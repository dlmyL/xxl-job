package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.complete.XxlJobCompleter;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.util.DateUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 调度中心接收执行器回调信息的工作组件，当接收
 * 到执行器端定时任务执行结果回调请求后被激活。
 */
@Slf4j
public class JobCompleteHelper {

    @Getter
    private static JobCompleteHelper instance = new JobCompleteHelper();

    // ---------------------- monitor ----------------------

    /*
    回调线程池，负责把执行器发送回来的定时任务执行信息赋值给XxlJobLog
    对象中的成员变量，然后更新数据库中XxlJobLog的信息。
     */
    private ThreadPoolExecutor callbackThreadPool = null;

    // 监控线程，该线程的作用就是用来判断调度中心调度的哪些定时任务真的是失败了
    private Thread monitorThread;
    private volatile boolean toStop = false;

    public void start() {
        callbackThreadPool = new ThreadPoolExecutor(
                2,
                20,
                30L,
                TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(3000),
                r -> new Thread(r, "xxl-job, admin JobLosedMonitorHelper-callbackThreadPool-" + r.hashCode()),
                (r, executor) -> {
                    r.run();
                    log.warn(">>>>>>>>>>> xxl-job, callback too fast, match threadpool rejected handler(run " + "now).");
                }
        );

        /*
        monitorThread线程启动之后，就会在一个循环中不断地从数据库中查找定时任务的执行信息，
        并且查找的时候，会以当前时间为标志，查找到当前时间的十分钟之前调度的所有定时任务的信息，
        当然，返回的就是这些定时任务的ID。为什么要这么查找呢？原因其实也不复杂，如果一个定时任
        务被调度了十分钟了，仍然没有收到执行结果，那这个定时任务的执行肯定就出问题了呀。
         */
        monitorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                /*
                这里休眠一会儿是因为需要等待JobTriggerPoolHelper组件初始化，
                因为不执行远程调度，也就没有回调过来的定时任务执行结果信息。
                 */
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (InterruptedException e) {
                    if (!toStop) {
                        log.error(e.getMessage(), e);
                    }
                }

                while (!toStop) {
                    try {
                        /*
                        这里得到一个时间信息，就是当前时间向前10分钟的时间，
                        这里传进去的参数-10，就是减10分钟的意思
                         */
                        Date losedTime = DateUtil.addMinutes(new Date(), -10);
                        /*
                        这里最后对应的就是这条SQL：
                        SELECT t.id
                        FROM xxl_job_log t
                        LEFT JOIN xxl_job_registry t2
                        ON t.executor_address = t2.registry_value
                        WHERE t.trigger_code = 200
                        AND t.handle_code = 0
                        AND t.trigger_time <=  #{losedTime}
                        AND t2.id IS NULL;
                        其实就是判断了一下，现在数据库中XxlJobLog的触发时间，其实就可以当做定时任务在调度中心
                        开始执行的那个时间，这里其实就是把当前时间前十分钟内提交执行的定时任务，但是始终没有得
                        到执行器回调的执行结果的定时任务全找出来了，因为t.handle_code=0，并且注册表中也没有对
                        应的数据了，说明心跳断了，具体的方法在XxlJobLogMapper中。
                         */
                        List<Long> losedJobIds = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().findLostJobIds(losedTime);
                        if (losedJobIds != null && losedJobIds.size() > 0) {
                            // 遍历已经执行了10分钟，但是还没有结果的任务
                            for (Long logId : losedJobIds) {
                                // 构建任务执行日志对象
                                XxlJobLog jobLog = new XxlJobLog();
                                jobLog.setId(logId);
                                jobLog.setHandleTime(new Date());
                                jobLog.setHandleCode(ReturnT.FAIL_CODE);
                                jobLog.setHandleMsg(I18nUtil.getString("joblog_lost_fail"));
                                /*
                                更新失败的定时任务状态，如果有子任务，会先调用
                                UPDATE xxl_job_log
                                SET `handle_time`= #{handleTime},
                                    `handle_code`= #{handleCode},
                                    `handle_msg`= #{handleMsg}
                                WHERE `id`= #{id}
                                 */
                                XxlJobCompleter.updateHandleInfoAndFinish(jobLog);
                            }
                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            log.error(">>>>>>>>>>> xxl-job, job fail monitor thread error:{}", e.getMessage(), e);
                        }
                    }

                    try {
                        // 线程睡眠60s，也就是该线程每60s才工作一次
                        TimeUnit.SECONDS.sleep(60);
                    } catch (Exception e) {
                        if (!toStop) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }

                log.info(">>>>>>>>>>> xxl-job, JobLosedMonitorHelper stop");
            }
        });

        // 设置monitorThread为守护线程
        monitorThread.setDaemon(true);
        monitorThread.setName("xxl-job, admin JobLosedMonitorHelper");
        monitorThread.start();
    }

    public void toStop() {
        toStop = true;
        callbackThreadPool.shutdownNow();
        monitorThread.interrupt();
        try {
            monitorThread.join();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }


    // ---------------------- helper ----------------------

    /** 处理服务端定时任务执行结果的回调请求 */
    public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {
        // 对回调请求做异步处理，更新日志中的调用结果
        callbackThreadPool.execute(() -> {
            for (HandleCallbackParam handleCallbackParam : callbackParamList) {
                // ==处理回调==
                ReturnT<String> callbackResult = callback(handleCallbackParam);
                log.debug(">>>>>>>>> JobApiController.callback {}, handleCallbackParam={}, callbackResult={}", (callbackResult.getCode() == ReturnT.SUCCESS_CODE ? "success" : "fail"), handleCallbackParam, callbackResult);
            }
        });

        return ReturnT.SUCCESS;
    }

    private ReturnT<String> callback(HandleCallbackParam handleCallbackParam) {
        /*
        得到对应的XxlJobLog对象:
        SELECT *
		FROM  xxl_job_log AS t
		WHERE t.id = #{id}
         */
        XxlJobLog log = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().load(handleCallbackParam.getLogId());
        if (log == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "log item not found.");
        }

        /*
        判断日志对象的处理结果码。
        因为这个响应码无论是哪种情况都是大于0的，如果大于0了，说明
        已经回调一次了，如果等于0，说明还没得到回调信息，任务也可能
        还处于运行中的状态。
         */
        if (log.getHandleCode() > 0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "log repeate callback.");
        }

        // 拼接信息
        StringBuffer handleMsg = new StringBuffer();
        if (log.getHandleMsg() != null) {
            handleMsg.append(log.getHandleMsg()).append("<br>");
        }
        if (handleCallbackParam.getHandleMsg() != null) {
            handleMsg.append(handleCallbackParam.getHandleMsg());
        }

        log.setHandleTime(new Date());
        // 在这里把定时任务执行的状态码赋值给XxlJobLog对象中的handleCode成员变量了
        log.setHandleCode(handleCallbackParam.getHandleCode());
        log.setHandleMsg(handleMsg.toString());

        /*
        更新数据库中的日志信息:
        UPDATE xxl_job_log
        SET `handle_time`= #{handleTime},
            `handle_code`= #{handleCode},
            `handle_msg` = #{handleMsg}
        WHERE `id`= #{id}
         */
        XxlJobCompleter.updateHandleInfoAndFinish(log);

        return ReturnT.SUCCESS;
    }
}
