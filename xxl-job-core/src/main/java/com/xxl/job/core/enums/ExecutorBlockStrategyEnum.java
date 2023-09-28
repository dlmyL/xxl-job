package com.xxl.job.core.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * <h1>阻塞处理策略</h1>
 */
@AllArgsConstructor
public enum ExecutorBlockStrategyEnum {

    /**
     * 串行
     */
    SERIAL_EXECUTION("Serial execution"),
    /*CONCURRENT_EXECUTION("并行"),*/
    /**
     * 丢弃
     */
    DISCARD_LATER("Discard Later"),
    /**
     * 覆盖
     */
    COVER_EARLY("Cover Early");

    @Getter
    @Setter
    private String title;

    public static ExecutorBlockStrategyEnum match(String name, ExecutorBlockStrategyEnum defaultItem) {
        if (name != null) {
            for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
                if (item.name().equals(name)) {
                    return item;
                }
            }
        }
        return defaultItem;
    }
}
