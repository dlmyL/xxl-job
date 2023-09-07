package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.*;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * <h1>xxl-job 服务端的启动类，在该类的 init 方法中会初始化各个组件</h1>
 *
 * @author xuxueli 2018-10-28 00:18:17
 */
public class XxlJobScheduler  {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);

    /**
     * <h2>初始化服务端的各个组件</h2>
     */
    public void init() throws Exception {
        // 初始化语言国际化的操作，其实内部就是把一些策略的中文初始化好
        initI18n();

        // 初始化触发线程池，这里面会创建两个线程池，一个快线程池，一个慢线程池
        // 触发器任务的执行就是由这两个线程池执行的
        JobTriggerPoolHelper.toStart();

        // 初始化注册中心组件
        JobRegistryHelper.getInstance().start();

        // 该组件的功能就是当调度中心调度任务失败的时候，发送邮件使用的
        JobFailMonitorHelper.getInstance().start();

        // 启动调度中心接收执行器回调信息的工作组件
        JobCompleteHelper.getInstance().start();

        // 统计定时任务日志的信息、成功失败次数等等
        // 同时也会清除过期日志，过期日志时间是用户写在配置文件中的，默认为 30 天
        JobLogReportHelper.getInstance().start();

        // 初始化任务调度线程，这个线程可以说是 xxl-job 服务端的核心了
        // 注意：所谓的任务调度就是哪个任务该执行了，这个线程就会把该任务提交了去执行，这就是调度的含义
        // 这个线程会一直扫描判断哪些任务应该执行了，这里面会用到【时间轮】
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init xxl-job admin success.");
    }

    /**
     * <h2>释放资源</h2>
     */
    public void destroy() throws Exception {

        // stop-schedule
        JobScheduleHelper.getInstance().toStop();

        // admin log report stop
        JobLogReportHelper.getInstance().toStop();

        // admin lose-monitor stop
        JobCompleteHelper.getInstance().toStop();

        // admin fail-monitor stop
        JobFailMonitorHelper.getInstance().toStop();

        // admin registry stop
        JobRegistryHelper.getInstance().toStop();

        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();

    }

    // ---------------------- I18n ----------------------

    /**
     * <h2>把阻塞策略中的中文初始化好</h2>
     */
    private void initI18n(){
        for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------

    /**
     * 这个就是远程调用的 Mqp 集合，在这个集合中存储的就是专门用来远程调用的客户端
     * 这里的 key 是远程调用的服务实例的地址，value 就是对应的客户端
     * 【注意】在 xxl-job 中进行远程调用，实际上使用的是 HTTP，在执行器那一侧，使用的
     *        也是由 Netty 构建的 HTTP 服务器
     */
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();
    /**
     * <h2>通过这个方法可以获得一个进行远程调用的客户端</h2>
     * 所谓的客户端和服务端都是相对的，当然真正的服务端并发压力会大很多，但是仅从收发消息的角度来说，
     * 客户端和服务端都是可以收发消息的
     */
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // 判断远程地址是否为空
        if (address==null || address.trim().length()==0) {
            return null;
        }

        // 规整一下地址，去掉空格
        address = address.trim();
        // 从远程调用的 Mqp 集合中获得远程调用的客户端
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            // 如果有就直接返回
            return executorBiz;
        }

        // 如果没有就创建一个客户端，然后存放到 Mqp 中
        // 这个时候，本来作为客户端的执行器，在使用 Netty 构建了服务器端后，又拥有服务端的身份了
        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());

        // 把创建好的客户端放入 Map 中
        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
