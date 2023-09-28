package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.util.I18nUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <h1>该类是当调度中心调度任务失败的时候，发送邮件用的</h1>
 */
@Slf4j
public class JobFailMonitorHelper {

    private static JobFailMonitorHelper instance = new JobFailMonitorHelper();

    public static JobFailMonitorHelper getInstance() {
        return instance;
    }

    // ---------------------- monitor ----------------------

	// 处理失败任务告警的线程
    private Thread monitorThread;
	// 线程是否停止工作
    private volatile boolean toStop = false;

    public void start() {
        monitorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!toStop) {
                    try {
						/*
						从数据库中查询执行失败的任务，查询的数量为1000
							SELECT id FROM `xxl_job_log`
							WHERE !(
								(trigger_code in (0, 200) and handle_code = 0)
								OR
								(handle_code = 200)
							)
							AND `alarm_status` = 0
							ORDER BY id ASC
							LIMIT 1000
						 【注意】这里查询出来的都是执行失败并且报警状态码还未改变的定时任务
						 */
                        List<Long> failLogIds = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao()
								.findFailJobLogIds(1000);
						// 如果结果不为空，说明存在执行失败的定时任务，并且报警状态码还未改变
                        if (failLogIds != null && !failLogIds.isEmpty()) {
							// 遍历该集合
                            for (long failLogId : failLogIds) {
								/*
									UPDATE xxl_job_log
									SET `alarm_status` = #{newAlarmStatus}
									WHERE `id`= #{logId} AND `alarm_status` = #{oldAlarmStatus}
									在这里把XxlJobLog的alarmStatus修改为-1，-1就是锁定状态，这里大家其实就可以把这个-1看成CAS的条件
									告警状态：0-默认、-1=锁定状态、1-无需告警、2-告警成功、3-告警失败
								 */
                                int lockRet = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao()
										.updateAlarmStatus(failLogId, 0, -1);
                                if (lockRet < 1) {
									// 走到这里说明更新数据库失败了，直接跳出本次循环，继续下一次循环
                                    continue;
                                }
								// 这里其实就是根据XxlJobLog的主键ID获得对应的XxlJobLog
                                XxlJobLog log = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().load(failLogId);
								// 根据定时任务ID得到具体的定时任务信息，当然，得到的都是执行失败的定时任务的具体信息
                                XxlJobInfo info = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(log.getJobId());

                                // 1、判断该定时任务的失败重试次数是否大于0
                                if (log.getExecutorFailRetryCount() > 0) {
                                    // EXEC JobTriggerPoolHelper#trigger
									// 如果大于0就立刻远程调度一次
									// 【注意】log.getExecutorFailRetryCount()-1这行代码，就会在每次重试的时候把重试次数减1，直到为0
                                    JobTriggerPoolHelper.trigger(log.getJobId(), TriggerTypeEnum.RETRY,
											(log.getExecutorFailRetryCount() - 1), log.getExecutorShardingParam(),
											log.getExecutorParam(), null);
									// 记录下来失败重试调用了一次
                                    String retryMsg = "<br><br><span style=\"color:#F39C12;\" > >>>>>>>>>>>" + I18nUtil.getString("jobconf_trigger_type_retry") + "<<<<<<<<<<< </span><br>";
                                    log.setTriggerMsg(log.getTriggerMsg() + retryMsg);
                                    // 更新数据库的信息，就是把XxlJobLog更新一下，因为这个定时任务的日志中记录了失败重试的信息
									XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateTriggerInfo(log);
                                }

                                // 2、定义一个新的报警状态
                                int newAlarmStatus = 0;        // 告警状态：0-默认、-1=锁定状态、1-无需告警、2-告警成功、3-告警失败
                                if (info != null) {
									// EXEC JobAlarmer#alarm
									// 如果查询到了执行失败的定时任务，就直接报警，发送告警邮件
                                    boolean alarmResult = XxlJobAdminConfig.getAdminConfig().getJobAlarmer().alarm(info, log);
									// 判断是否发送成功，如果发送成功就把报警状态设置为2，2就代表报警成功了，3就代表失败
                                    newAlarmStatus = alarmResult ? 2 : 3;
                                } else {
									// 如果没有得到对应的XxlJobInfo，就无须报警
									newAlarmStatus = 1;
                                }
								// 在这里把最新的状态吗更新到数据库，-1这个值也就不再使用了
                                XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateAlarmStatus(failLogId, -1, newAlarmStatus);
                            }
                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            log.error(">>>>>>>>>>> xxl-job, job fail monitor thread error:{}", e);
                        }
                    }
                    try {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (Exception e) {
                        if (!toStop) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
                log.info(">>>>>>>>>>> xxl-job, job fail monitor thread stop");
            }
        });
        monitorThread.setDaemon(true);
        monitorThread.setName("xxl-job, admin JobFailMonitorHelper");
        monitorThread.start();
    }

    public void toStop() {
        toStop = true;
        monitorThread.interrupt();
        try {
            monitorThread.join();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

}
