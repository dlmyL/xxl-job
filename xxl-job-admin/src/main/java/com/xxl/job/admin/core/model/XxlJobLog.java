package com.xxl.job.admin.core.model;

import lombok.Data;

import java.util.Date;

/**
 * <h1>这是和日志有关的实体类</h1>
 */
@Data
public class XxlJobLog {
    // 日志 ID
    private long id;

    // 执行器组 ID
    private int jobGroup;
    // 定时任务 ID
    private int jobId;

    // 执行器地址
    private String executorAddress;
    // 封装定时任务的 JobHandler 名称
    private String executorHandler;
    // 执行器参数
    private String executorParam;
    // 执行器分片参数
    private String executorShardingParam;
    // 失败重试次数
    private int executorFailRetryCount;

    // 触发器触发时间
    private Date triggerTime;
    // 触发器任务的响应码
    private int triggerCode;
    // 触发任务信息
    private String triggerMsg;

    // 定时任务执行时间
    private Date handleTime;
    // 执行的响应码
    private int handleCode;
    // 执行的具体结果
    private String handleMsg;

    // 警报的状态码，0是默认，1是不需要报警，2是报警成功，3是报警失败
    private int alarmStatus;

}
