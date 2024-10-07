package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.JobCompleteHelper;
import com.xxl.job.admin.core.thread.JobFailMonitorHelper;
import com.xxl.job.admin.core.thread.JobLogReportHelper;
import com.xxl.job.admin.core.thread.JobRegistryHelper;
import com.xxl.job.admin.core.thread.JobScheduleHelper;
import com.xxl.job.admin.core.thread.JobTriggerPoolHelper;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * XXL-JOB调度中心端的启动类，在该类的#init
 * 方法中会初始化各个组件。
 */
@Slf4j
public class XxlJobScheduler {

    public XxlJobScheduler() {

    }

    public void init() throws Exception {
        // 初始化语言国际化的操作，其实内部就是把一些策略的中文初始化好
        initI18n();

        /*
        初始化任务触发线程池，这里面会创建两个线程池，一个快线程池，一个
        慢线程池，触发器任务的执行就是由这两个线程池负责的。
         */
        JobTriggerPoolHelper.toStart();

        // 初始化注册中心组件
        JobRegistryHelper.getInstance().start();

        // 该组件的功能就是当调度中心调度任务失败的时候，发送邮件使用的
        JobFailMonitorHelper.getInstance().start();

        // 启动调度中心接收执行器回调信息的工作组件
        JobCompleteHelper.getInstance().start();

        /*
        统计定时任务日志的信息、成功失败次数等等，同时也会清除过期日志，
        过期日志时间是用户写在配置文件中的，默认为30天。
         */
        JobLogReportHelper.getInstance().start();

        /*
        初始化任务调度线程，这个线程可以说是xxl-job服务端的核心了。
        注意：所谓的任务调度就是哪个任务该执行了，这个线程就会把该
        任务提交了去执行，这就是调度的含义，这个线程会一直扫描判断
        哪些任务应该执行了，这里面会用到时间轮。
         */
        JobScheduleHelper.getInstance().start();

        log.info(">>>>>>>>> init xxl-job admin success.");
    }

    public void destroy() throws Exception {
        JobScheduleHelper.getInstance().toStop();
        JobLogReportHelper.getInstance().toStop();
        JobCompleteHelper.getInstance().toStop();
        JobFailMonitorHelper.getInstance().toStop();
        JobRegistryHelper.getInstance().toStop();
        JobTriggerPoolHelper.toStop();
    }

    // ---------------------- I18n ----------------------

    /**
     * 把阻塞策略中的中文初始化好
     */
    private void initI18n() {
        for (ExecutorBlockStrategyEnum item : ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------

    /**
     * 这个就是远程调用的Map缓存，在这个集合中存储的就是专门用来远程调用的客户端。
     * key是远程调用的服务实例的地址，value是对应的客户端对象。
     * 注意：在xxl-job中进行远程调用，实际上使用的是HTTP，在执行器那一侧，使用的
     * 也是由Netty构建的HTTP服务器。
     */
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<>();

    /**
     * 通过这个方法可以获得一个进行远程调用的客户端。
     * 所谓的客户端和服务端都是相对的，当然真正的服务端并发压力会大很多，
     * 但是仅从收发消息的角度来说，客户端和服务端都是可以收发消息的。
     */
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // 判断远程地址是否为空
        if (address == null || address.trim().length() == 0) {
            return null;
        }
        // 规整一下地址，去掉空格
        address = address.trim();

        /*
        从远程调用的executorBizRepository集合中获得远程调用的客户端，如果
        存在就直接返回，如果不存在就创建一个客户端，然后存放到缓存起来。
         */
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());
        executorBizRepository.put(address, executorBiz);

        return executorBiz;
    }
}
