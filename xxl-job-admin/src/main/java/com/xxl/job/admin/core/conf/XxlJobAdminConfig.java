package com.xxl.job.admin.core.conf;

import com.xxl.job.admin.core.alarm.JobAlarmer;
import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.dao.*;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.Arrays;

/**
 * KEYPOINT 调度中心组件启动入口
 * <h1>
 *     这个类是服务端的启动入口，该类实现了 Spring 的 InitializingBean 接口，所以该类中的 afterPropertiesSet
 *     方法会在容器中的 BEAN 初始化完毕后被回调，回调过程中会创建 XXL-JOB 中最重要的快慢线程池，同时也会启动 XXL-JOB
 *     中的时间轮
 * </h1>
 *
 * @author xuxueli 2017-04-28
 */

@Component
public class XxlJobAdminConfig implements InitializingBean, DisposableBean {

    private static XxlJobAdminConfig adminConfig = null;
    public static XxlJobAdminConfig getAdminConfig() {
        return adminConfig;
    }


    // ---------------------- XxlJobScheduler ----------------------

    private XxlJobScheduler xxlJobScheduler;

    @Override
    public void afterPropertiesSet() throws Exception {
        /*
        为什么这里可以直接赋值呢？
            跟 Spring 的 BEAN 对象的初始化有关，XxlJobAdminConfig 中添加了 @Component 注解，所以会
            作为 BEAN 被反射创建，创建的时候会调用无参构造器。
            而 afterPropertiesSet 方法实在容器所有的 BEAN 初始化完成时才会被回调，所以这时候 XxlJobAdminConfig
            对象已经创建完成了，直接赋值 this 就行了
         */
        adminConfig = this;
        // 这里就是把调度器对象创建出来，在调度器的 init 方法中，服务端的各个组件就启动成功了
        xxlJobScheduler = new XxlJobScheduler();
        // 初始化服务端的各个组件，在这里，服务端才算是真正开始工作了
        xxlJobScheduler.init();
    }

    /**
     * <h2>当该类的对象被销毁的时候，会调用此方法</h2>
     */
    @Override
    public void destroy() throws Exception {
        /*
        调动调度器的销毁方法，该方法实际上就是注销之前初始化的一些组件
        说的直接一点，就是把组件中的线程池资源释放了，让线程池关闭，停止工作
        当然，这里释放的并不是所有组件的线程资源
         */
        xxlJobScheduler.destroy();
    }


    // ---------------------- XxlJobScheduler ----------------------

    // conf
    @Value("${xxl.job.i18n}")
    private String i18n;

    @Value("${xxl.job.accessToken}")
    private String accessToken;

    @Value("${spring.mail.from}")
    private String emailFrom;

    /**
     * 快线程池的最大线程数
     */
    @Value("${xxl.job.triggerpool.fast.max}")
    private int triggerPoolFastMax;

    /**
     * 慢线程池的最大线程数
     */
    @Value("${xxl.job.triggerpool.slow.max}")
    private int triggerPoolSlowMax;

    @Value("${xxl.job.logretentiondays}")
    private int logretentiondays;

    // dao, service

    @Resource
    private XxlJobLogDao xxlJobLogDao;
    @Resource
    private XxlJobInfoDao xxlJobInfoDao;
    @Resource
    private XxlJobRegistryDao xxlJobRegistryDao;
    @Resource
    private XxlJobGroupDao xxlJobGroupDao;
    @Resource
    private XxlJobLogReportDao xxlJobLogReportDao;
    @Resource
    private JavaMailSender mailSender;
    @Resource
    private DataSource dataSource;
    @Resource
    private JobAlarmer jobAlarmer;


    public String getI18n() {
        if (!Arrays.asList("zh_CN", "zh_TC", "en").contains(i18n)) {
            return "zh_CN";
        }
        return i18n;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getEmailFrom() {
        return emailFrom;
    }

    public int getTriggerPoolFastMax() {
        if (triggerPoolFastMax < 200) {
            return 200;
        }
        return triggerPoolFastMax;
    }

    public int getTriggerPoolSlowMax() {
        if (triggerPoolSlowMax < 100) {
            return 100;
        }
        return triggerPoolSlowMax;
    }

    public int getLogretentiondays() {
        if (logretentiondays < 7) {
            return -1;  // Limit greater than or equal to 7, otherwise close
        }
        return logretentiondays;
    }

    public XxlJobLogDao getXxlJobLogDao() {
        return xxlJobLogDao;
    }

    public XxlJobInfoDao getXxlJobInfoDao() {
        return xxlJobInfoDao;
    }

    public XxlJobRegistryDao getXxlJobRegistryDao() {
        return xxlJobRegistryDao;
    }

    public XxlJobGroupDao getXxlJobGroupDao() {
        return xxlJobGroupDao;
    }

    public XxlJobLogReportDao getXxlJobLogReportDao() {
        return xxlJobLogReportDao;
    }

    public JavaMailSender getMailSender() {
        return mailSender;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public JobAlarmer getJobAlarmer() {
        return jobAlarmer;
    }

}
