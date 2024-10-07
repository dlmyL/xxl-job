package com.xxl.job.admin.core.trigger;

import com.xxl.job.admin.core.util.I18nUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 触发枚举类型，这个其实还是对应着触发器任务的类型
 * 一般都是cron类型的
 */
@Getter
@AllArgsConstructor
public enum TriggerTypeEnum {

    MANUAL(I18nUtil.getString("jobconf_trigger_type_manual")),
    CRON(I18nUtil.getString("jobconf_trigger_type_cron")),
    RETRY(I18nUtil.getString("jobconf_trigger_type_retry")),
    PARENT(I18nUtil.getString("jobconf_trigger_type_parent")),
    API(I18nUtil.getString("jobconf_trigger_type_api")),
    MISFIRE(I18nUtil.getString("jobconf_trigger_type_misfire"));

    private final String title;
}
