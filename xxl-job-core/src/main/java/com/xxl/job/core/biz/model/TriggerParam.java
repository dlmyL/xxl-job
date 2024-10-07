package com.xxl.job.core.biz.model;

import lombok.Data;

import java.io.Serializable;

/**
 * 封装触发器信息的实体类，当调度中心远程调用任务时，会发送给执行器触发器参数，就是这个类的对象
 * 调度中心 => 执行器 执行任务调度传递的对象
 */
@Data
public class TriggerParam implements Serializable {

    private static final long serialVersionUID = 42L;

    private int jobId; // 定时任务 ID


    // === 执行器相关 ===

    private String executorHandler;       // JobHandler的名字，也就是定时任务方法的名字
    private String executorParams;        // 定时任务参数
    private String executorBlockStrategy; // 阻塞策略
    private int executorTimeout;          // 超时时间


    // === 日志相关 ===

    private long logId;                   // 日志ID
    private long logDateTime;             // 日志时间


    // === 执行模式相关 ===

    private String glueType;              // Glue运行模式
    private String glueSource;            // Glue代码文本
    private long glueUpdatetime;          // Glue更新时间


    // === 分片相关 ===

    private int broadcastIndex;           // 分片索引
    private int broadcastTotal;           // 分片总数

}
