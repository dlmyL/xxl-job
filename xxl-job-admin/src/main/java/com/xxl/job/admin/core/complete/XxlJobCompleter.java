package com.xxl.job.admin.core.complete;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.thread.JobTriggerPoolHelper;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.context.XxlJobContext;

import java.text.MessageFormat;

/**
 * 更新日志信息，触发子任务的类
 */
public class XxlJobCompleter {

    public static int updateHandleInfoAndFinish(XxlJobLog xxlJobLog) {
        // 触发子任务
        finishJob(xxlJobLog);

        // 判断字符串长度，太长的话需要截取一段
        if (xxlJobLog.getHandleMsg().length() > 15000) {
            xxlJobLog.setHandleMsg(xxlJobLog.getHandleMsg().substring(0, 15000));
        }

        /*
        更新数据库：
        UPDATE xxl_job_log
        SET `handle_time`= #{handleTime},
            `handle_code`= #{handleCode},
            `handle_msg` = #{handleMsg}
        WHERE `id`= #{id}
         */
        return XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateHandleInfo(xxlJobLog);
    }

    /**
     * 触发子任务的方法
     */
    private static void finishJob(XxlJobLog xxlJobLog) {
        String triggerChildMsg = null;
        // 先判断定时任务是不是执行成功的状态
        if (XxlJobContext.HANDLE_CODE_SUCCESS == xxlJobLog.getHandleCode()) {
            /*
            如果定时任务执行成功了，就先得到该定时任务的具体信息：
                SELECT *
                FROM  xxl_job_info AS t
                WHERE t.id = #{id}
             */
            XxlJobInfo xxlJobInfo = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(xxlJobLog.getJobId());
            if (xxlJobInfo != null
                    && xxlJobInfo.getChildJobId() != null
                    && xxlJobInfo.getChildJobId().trim().length() > 0) {
                triggerChildMsg = "<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>" + I18nUtil.getString("jobconf_trigger_child_run") + "<<<<<<<<<<< </span><br>";
                // 如果有多个子任务，就切分子任务ID 数组
                String[] childJobIds = xxlJobInfo.getChildJobId().split(",");
                // 遍历子任务 D数组
                for (int i = 0; i < childJobIds.length; i++) {
                    // 得到子任务ID
                    int childJobId = (childJobIds[i] != null && childJobIds[i].trim().length() > 0
                            && isNumeric(childJobIds[i])) ? Integer.valueOf(childJobIds[i]) : -1;
                    if (childJobId > 0) {
                        // ==调度子任务==
                        JobTriggerPoolHelper.trigger(childJobId, TriggerTypeEnum.PARENT, -1,
                                null, null, null);
                        // 设置调度成功的结果
                        ReturnT<String> triggerChildResult = ReturnT.SUCCESS;
                        triggerChildMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg1"),
                                (i + 1), childJobIds.length, childJobIds[i],
                                (triggerChildResult.getCode() == ReturnT.SUCCESS_CODE ?
                                        I18nUtil.getString("system_success") : I18nUtil.getString("system_fail")),
                                triggerChildResult.getMsg());
                    } else {
                        triggerChildMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg2"), (i + 1), childJobIds.length, childJobIds[i]);
                    }
                }
            }
        }

        if (triggerChildMsg != null) {
            xxlJobLog.setHandleMsg(xxlJobLog.getHandleMsg() + triggerChildMsg);
        }
    }

    private static boolean isNumeric(String str) {
        try {
            int result = Integer.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
