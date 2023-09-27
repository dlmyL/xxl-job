package com.xxl.job.admin.core.alarm;

import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <h1>这个类是用来发送报警邮件的，但实际上真正的功能实在 EmailJobAlarm 中实现的</h1>
 */
@Component
public class JobAlarmer implements ApplicationContextAware, InitializingBean {
    private static Logger logger = LoggerFactory.getLogger(JobAlarmer.class);

    /**
     * spring 容器
     */
    private ApplicationContext applicationContext;
    /**
     * 邮件报警器集合
     */
    private List<JobAlarm> jobAlarmList;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * <h2>该方法会在容器中的 BEAN 初始化完毕后被调用</h2>
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        // 把容器中所有的邮件报警器收集到 jobAlarmList 中
        Map<String, JobAlarm> serviceBeanMap = applicationContext.getBeansOfType(JobAlarm.class);
        if (serviceBeanMap != null && serviceBeanMap.size() > 0) {
            jobAlarmList = new ArrayList<JobAlarm>(serviceBeanMap.values());
        }
    }

    /**
     * <h2>在 JobFailMonitorHelper 类中被调用到的发送报警邮件的方法</h2>
     */
    public boolean alarm(XxlJobInfo info, XxlJobLog jobLog) {
        boolean result = false;
        // 先判断邮件报警器集合是否为空
        if (jobAlarmList!=null && jobAlarmList.size()>0) {
            // 不为空就先设置所有报警器发送结果都为成功
            result = true;
            // 遍历邮件报警器
            for (JobAlarm alarm: jobAlarmList) {
                // 设置发送结果为 false
                boolean resultItem = false;
                try {
                    // 在这里真正发送报警邮件给用户，然后返回给用户发送结果
                    resultItem = alarm.doAlarm(info, jobLog);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                if (!resultItem) {
                    // 在这里可以看到，如果发送失败，就把最开始设置的 result 重新改为 false
                    // 并且这里可以明白，只要有一个报警器发送邮件失败，总的发送结果就会被设置为失败
                    result = false;
                }
            }
        }

        return result;
    }

}
