package com.xxl.job.core.biz.model;

import lombok.Data;

import java.io.Serializable;

/**
 * <h1>封装触发器信息的实体类，当调度中心远程调用任务时，会发送给执行器触发器参数，就是这个类的对象</h1>
 */
@Data
public class TriggerParam implements Serializable{

    private static final long serialVersionUID = 42L;
    /**
     * 定时任务 ID
     */
    private int jobId;

    /**
     * JobHandler 的名字
     */
    private String executorHandler;
    /**
     * 定时任务参数
     */
    private String executorParams;
    /**
     * 阻塞策略
     */
    private String executorBlockStrategy;
    /**
     * 超时时间
     */
    private int executorTimeout;

    /**
     * 日志 ID
     */
    private long logId;
    /**
     * 日志时间
     */
    private long logDateTime;

    /**
     * 运行模式
     */
    private String glueType;
    /**
     * 代码文本
     */
    private String glueSource;
    /**
     * gule 更新时间
     */
    private long glueUpdatetime;

    /**
     * 分片索引
     */
    private int broadcastIndex;
    /**
     * 分片总数
     */
    private int broadcastTotal;

}
