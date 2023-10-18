package com.xxl.job.core.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * <h1>阻塞处理策略</h1>
 * <p>
 * 场景：如果有一个定时任务每隔5秒就要执行一次，调度任务调度了定时任务，但是出于某种原因，本次调度的定时任务耗时比较长，
 * 可能过了6秒还没有执行完，那么下一次定时任务调度的时候，这个耗时很长的定时任务仍然在执行。
 * 这样一来，后面调度的定时任务就无法执行了，这就是从定时任务执行角度发生的阻塞。
 * </p>
 */
@AllArgsConstructor
public enum ExecutorBlockStrategyEnum {

    /**
     * 单机串行（默认）
     * 前一个定时任务没有执行完，那么就把现在调度的定时任务的信息存放到定时任务对应的工作线程内部的任务队列中，慢慢等待调度
     */
    SERIAL_EXECUTION("Serial execution"),
    /**
     * 丢弃后续调度
     * 在执行器这一端，如果检测到工作线程内的任务队列中有数据，说明还有被调度的定时任务尚未执行，
     * 如果是这种情况，那么本次被调度的定时任务就不会被执行，并且立刻返回给调度中心一个调度失败的结果
     */
    DISCARD_LATER("Discard Later"),
    /**
     * 覆盖之前调度
     * 当前正在执行的定时任务就不会再被执行了，而是直接开始执行本次调度的定时任务。
     * 实际上就是把定时任务对应的工作线程置为null，然后再创建一个新的工作线程，然后直接执行本次调度的定时任务
     */
    COVER_EARLY("Cover Early");

    @Getter
    @Setter
    private String title;

    public static ExecutorBlockStrategyEnum match(String name, ExecutorBlockStrategyEnum defaultItem) {
        if (name != null) {
            for (ExecutorBlockStrategyEnum item : ExecutorBlockStrategyEnum.values()) {
                if (item.name().equals(name)) {
                    return item;
                }
            }
        }
        return defaultItem;
    }

}
