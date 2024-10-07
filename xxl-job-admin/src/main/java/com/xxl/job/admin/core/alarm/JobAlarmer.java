package com.xxl.job.admin.core.alarm;

import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 这个类是用来发送报警邮件的，但实际上真正的功能实在EmailJobAlarm中实现的。
 */
@Slf4j
@Component
public class JobAlarmer implements ApplicationContextAware, InitializingBean {

    // 邮件报警器集合
    private List<JobAlarm> jobAlarmList;

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // 把所有的邮件报警器收集到jobAlarmList中
        Map<String, JobAlarm> serviceBeanMap = applicationContext.getBeansOfType(JobAlarm.class);
        if (serviceBeanMap != null && serviceBeanMap.size() > 0) {
            jobAlarmList = new ArrayList<>(serviceBeanMap.values());
        }
    }

    public boolean alarm(XxlJobInfo info, XxlJobLog jobLog) {
        boolean result = false;
        // 先判断邮件报警器集合是否为空
        if (jobAlarmList != null && jobAlarmList.size() > 0) {
            // 不为空就先设置所有报警器发送结果都为成功
            result = true;
            // 遍历邮件报警器
            for (JobAlarm alarm : jobAlarmList) {
                // 设置发送结果为false
                boolean resultItem = false;
                try {
                    // 在这里真正发送报警邮件给用户，然后返回给用户发送结果
                    resultItem = alarm.doAlarm(info, jobLog);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                if (!resultItem) {
                    /*
                    在这里可以看到，如果发送失败，就把最开始设置的result重新改为false，
                    并且这里可以明白，只要有一个报警器发送邮件失败，总的发送结果就会被
                    设置为失败。
                     */
                    result = false;
                }
            }
        }

        return result;
    }
}
