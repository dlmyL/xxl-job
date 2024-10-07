package com.xxl.job.admin.core.conf;

import com.xxl.job.admin.core.alarm.JobAlarmer;
import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.dao.*;
import lombok.Getter;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.Arrays;

/**
 * 这个类是服务端的启动入口，该类实现了Spring的InitializingBean接口，所以该类中
 * 的#afterPropertiesSet方法会在容器中的Bean初始化完毕后被回调，回调过程中会创建
 * XXL-JOB中最重要的快慢线程池，同时也会启动XXL-JOB中的时间轮。
 */
@Component
public class XxlJobAdminConfig implements InitializingBean, DisposableBean {

    @Getter
    private static XxlJobAdminConfig adminConfig = null;

    private XxlJobScheduler xxlJobScheduler;

    @Override
    public void afterPropertiesSet() throws Exception {
        /*
        为什么这里可以直接赋值呢？
        跟Spring的BEAN对象的初始化有关，XxlJobAdminConfig中添加了@Component注解，
        所以会作为BEAN被反射创建，创建的时候会调用无参构造器。
        而afterPropertiesSet方法实在容器所有的BEAN初始化完成时才会被回调，所以这时
        候XxlJobAdminConfig对象已经创建完成了，直接赋值this就行了。
        */
        adminConfig = this;

        // 这里就是把调度器对象创建出来，在调度器的init方法中，会启动服务端的各个组件
        xxlJobScheduler = new XxlJobScheduler();
        // ==启动调度中心的各个组件==
        xxlJobScheduler.init();
    }

    @Override
    public void destroy() throws Exception {
        /*
        调动调度器的销毁方法，该方法实际上就是注销之前初始化的一些组件，
        说的直接一点，就是把组件中的线程池资源释放了，让线程池关闭，停
        止工作，当然，这里释放的并不是所有组件的线程资源。
        */
        xxlJobScheduler.destroy();
    }

    // conf
    @Value("${xxl.job.i18n}")
    private String i18n;
    @Getter
    @Value("${xxl.job.accessToken}")
    private String accessToken;
    @Getter
    @Value("${spring.mail.from}")
    private String emailFrom;
    /** 快线程池的最大线程数 */
    @Value("${xxl.job.triggerpool.fast.max}")
    private int triggerPoolFastMax;
    /** 慢线程池的最大线程数 */
    @Value("${xxl.job.triggerpool.slow.max}")
    private int triggerPoolSlowMax;
    @Value("${xxl.job.logretentiondays}")
    private int logretentiondays;

    // dao, service
    @Getter
    @Resource
    private XxlJobLogDao xxlJobLogDao;
    @Getter
    @Resource
    private XxlJobInfoDao xxlJobInfoDao;
    @Getter
    @Resource
    private XxlJobRegistryDao xxlJobRegistryDao;
    @Getter
    @Resource
    private XxlJobGroupDao xxlJobGroupDao;
    @Getter
    @Resource
    private XxlJobLogReportDao xxlJobLogReportDao;
    @Getter
    @Resource
    private JavaMailSender mailSender;
    @Getter
    @Resource
    private DataSource dataSource;
    @Getter
    @Resource
    private JobAlarmer jobAlarmer;

    public String getI18n() {
        if (!Arrays.asList("zh_CN", "zh_TC", "en").contains(i18n)) {
            return "zh_CN";
        }
        return i18n;
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
            return -1;
        }
        return logretentiondays;
    }
}
