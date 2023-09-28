package com.xxl.job.core.biz;

import com.xxl.job.core.biz.model.*;

/**
 * 执行器 RESTful API，提供给调度端进行调用
 * API服务位置：com.xxl.job.core.biz.ExecutorBiz
 */
public interface ExecutorBiz {

    /**
     * 心跳检测
     * ------
     * 说明：    调度中心检测执行器是否在线时使用
     * 地址格式：{执行器内嵌服务根地址}/beat
     */
    ReturnT<String> beat();

    /**
     * 忙碌检测
     * ------
     * 说明：    调度中心检测指定执行器上指定任务是否忙碌（运行中）时使用
     * 地址格式：{执行器内嵌服务根地址}/idleBeat
     */
    ReturnT<String> idleBeat(IdleBeatParam idleBeatParam);

    /**
     * 触发任务
     * ------
     * 说明：    触发任务执行
     * 地址格式：{执行器内嵌服务根地址}/run
     */
    ReturnT<String> run(TriggerParam triggerParam);

    /**
     * 终止任务
     * ------
     * 说明：    终止任务
     * 地址格式：{执行器内嵌服务根地址}/kill
     */
    ReturnT<String> kill(KillParam killParam);

    /**
     * 查看执行日志
     * ------
     * 说明：    终止任务，滚动方式加载
     * 地址格式：{执行器内嵌服务根地址}/log
     */
    ReturnT<LogResult> log(LogParam logParam);

}
