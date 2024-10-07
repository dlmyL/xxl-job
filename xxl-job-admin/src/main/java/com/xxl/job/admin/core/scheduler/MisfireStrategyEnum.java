package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.util.I18nUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 定时任务调度失败策略
 */
@AllArgsConstructor
public enum MisfireStrategyEnum {

    /** 默认什么也不做 */
    DO_NOTHING(I18nUtil.getString("misfire_strategy_do_nothing")),

    /** 失败后马上重试一次 */
    FIRE_ONCE_NOW(I18nUtil.getString("misfire_strategy_fire_once_now"));

    @Getter
    private final String title;

    public static MisfireStrategyEnum match(String name, MisfireStrategyEnum defaultItem) {
        for (MisfireStrategyEnum item : MisfireStrategyEnum.values()) {
            if (item.name().equals(name)) {
                return item;
            }
        }
        return defaultItem;
    }
}
