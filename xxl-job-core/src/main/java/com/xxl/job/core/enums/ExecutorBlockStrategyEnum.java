package com.xxl.job.core.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * 任务阻塞处理策略。
 * 场景：如果有一个定时任务每隔5秒就要执行一次，调度任务调度了定时任务，但是出于某种
 * 原因，本次调度的定时任务耗时比较长，可能过了6秒还没有执行完，那么下一次定时任务调
 * 度的时候，这个耗时很长的定时任务仍然在执行，这样一来，后面调度的定时任务就无法执行
 * 了，这就是从定时任务执行角度发生的阻塞。
 */
@AllArgsConstructor
public enum ExecutorBlockStrategyEnum {

    /**
     * 丢弃后续调度，如果triggerQueue中还有正在执行的任务，则不将
     * 本次请求放入到队列中。
     * 在执行器这一端，如果检测到工作线程内的任务队列中有数据，说明
     * 还有被调度的定时任务尚未执行，如果是这种情况，那么本次被调度
     * 的定时任务就不会被执行，并且立刻返回给调度中心一个调度失败的
     * 结果。
     */
    DISCARD_LATER("Discard Later"),
    /**
     * 单机串行(默认)，将当次请求直接push到triggerQueue中。
     * 前一个定时任务没有执行完，那么就把现在调度的定时任务的信息
     * 存放到定时任务对应的工作线程内部的任务队列中，慢慢等待调度。
     */
    SERIAL_EXECUTION("Serial execution"),
    /**
     * 覆盖之前调度，重新创建一个jobThread执行任务，先前的线程会在执行
     * 完毕后，被下一次GC回收。
     * 当前正在执行的定时任务就不会再被执行了，而是直接开始执行本次调度
     * 的定时任务。实际上就是把定时任务对应的工作线程置为null，然后再创
     * 建一个新的工作线程，然后直接执行本次调度的定时任务。
     */
    COVER_EARLY("Cover Early");

    @Setter
    @Getter
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
