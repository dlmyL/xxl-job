package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.util.I18nUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * <h1>定时任务的调度类型</h1>
 */
@AllArgsConstructor
public enum ScheduleTypeEnum {

    /**
     * 不使用任何类型
     */
    NONE(I18nUtil.getString("schedule_type_none")),

    /**
     * 一般都是用 cron 表达式
     */
    CRON(I18nUtil.getString("schedule_type_cron")),

    /**
     * 按照固定频率
     */
    FIX_RATE(I18nUtil.getString("schedule_type_fix_rate")),

    /**
     * schedule by fix delay (in seconds)， after the last time
     */
    /*FIX_DELAY(I18nUtil.getString("schedule_type_fix_delay"))*/;

    @Getter
    private final String title;

    public static ScheduleTypeEnum match(String name, ScheduleTypeEnum defaultItem){
        for (ScheduleTypeEnum item: ScheduleTypeEnum.values()) {
            if (item.name().equals(name)) {
                return item;
            }
        }
        return defaultItem;
    }

}
