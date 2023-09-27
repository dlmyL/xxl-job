package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobRegistry;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.*;

/**
 * <h1>
 * 该组件会初始化和注册中心相关的线程，大家可以想一想，执行器要注册到服务端，这些工作肯定就需要专门的线程来工作。
 * 而当执行器注册成功之后，如果过了不久就掉线了，也就是心跳检测超时，结果服务器这边不知道，还持有者掉线的执行器的地址，
 * 这样一来，远程调用肯定是无法成功的。
 * 所以定期检查并清理掉线执行器也需要专门的线程来处理，这两个操作，就是本类的职责
 * </h1>
 */
public class JobRegistryHelper {

    private static Logger logger = LoggerFactory.getLogger(JobRegistryHelper.class);

    private static JobRegistryHelper instance = new JobRegistryHelper();

    public static JobRegistryHelper getInstance() {
        return instance;
    }

    // 该线程池的作用就是注册或者移除执行器地址的
    private ThreadPoolExecutor registryOrRemoveThreadPool = null;
    // 该线程的作用就是检测注册中心过期的执行器
    private Thread registryMonitorThread;

    private volatile boolean toStop = false;

    /**
     * <h2>创建并启动上面的线程池</h2>
     */
    public void start() {
        // 执行器注册和移除执行器地址任务的线程池在这里被创建了
        registryOrRemoveThreadPool = new ThreadPoolExecutor(
                2,
                10,
                30L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r,
                                "xxl-job, admin JobRegistryMonitorHelper-registryOrRemoveThreadPool-" + r.hashCode());
                    }
                },
                // 下面这个是xxl-job定义的线程池拒绝策略
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        // 其实就是把被拒绝的任务再执行一遍
                        r.run();
                        logger.warn(">>>>>>>>>>> xxl-job, registry or remove too fast, match threadpool rejected " +
                                "handler(run now).");
                    }
                });

        // 该线程就是用来循环检测注册中心注册的执行器是否过期，如果过期就移除过期数据，说白了，就是起到一个
        // 心跳检测的作用，该线程每次循环都会睡30s，其实就是30s检测一次过期的执行器
        registryMonitorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!toStop) {
                    try {
                        // 这里查询的是所有自动注册的执行器组，手动录入的执行器不在此查询范围内，所谓自动注册，就是执行器启动时通过http把注册
                        // 信息发送到调度中心的注册方式，并不是用户在web界面手动录入的注册方式
                        // 【注意】这里查询的是执行器组，还不是单个的执行器
                        List<XxlJobGroup> groupList =
								XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().findByAddressType(0);
                        if (groupList != null && !groupList.isEmpty()) {
                            // 这里的逻辑其实还要去对应的Mapper中查看：
                            // WHERE t.update_time < DATE_ADD(#{nowTime}, INTERVAL -#{timeout} SECOND)
                            // 其实就是判断数据库中记录的所有执行器的最新一次的更新时间是否小于当前时间减去90s，这就意味着
                            // 执行器的超时时间就是90s，只要在90s内，执行器没有再更新自己的信息，就意味着它停机了，而在执行器那一端，
                            // 是每30s就重新注册一次到注册中心
                            // 【注意】这里并没有区分是手动注册还是自动注册，只要是超时了的执行器都检测出来，然后从数据库中删除即可
                            List<Integer> ids =
									XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
                            if (ids != null && ids.size() > 0) {
                                // 根据过期执行器的id直接删除执行器
                                XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().removeDead(ids);
                            }

                            // 该Map是用来缓存appName和对应的执行器地址的
                            HashMap<String, List<String>> appAddressMap = new HashMap<>();
                            // 这里查出的就是所有没有过期的执行器，同样不考虑注册类型，是否自动注册或手动录入，对应的SQL如下：
                            // WHERE t.update_time > DATE_ADD(#{nowTime},INTERVAL -#{timeout} SECOND)
                            // 就是把小于号改成了大于号
                            List<XxlJobRegistry> list =
									XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
                            if (list != null) {
                                // 走到这里说明数据库中存在没有超时的执行器数据
                                for (XxlJobRegistry item : list) {
                                    // 遍历这些未过期的执行器，先判断注册类型
                                    if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
                                        // 如果是自动注册，就先获得执行器的项目名称，也就是appName
                                        String appname = item.getRegistryKey();
                                        // 以appName为key，判断刚才的Map中是否缓存着该appName对应的执行器地址
                                        List<String> registryList = appAddressMap.get(appname);
                                        if (registryList == null) {
                                            // 如果没有，则创建一个集合，这里之所以用到集合，是考虑到了定时任务很可能部署在多个执行器上，
                                            // 而相同定时任务的执行器名称是相同的，正好可以用来当做key，value就是不同的执行器地址
                                            registryList = new ArrayList<>();
                                        }
                                        // 如果创建的这个集合尚未包含当前循环的执行器地址
                                        if (!registryList.contains(item.getRegistryValue())) {
                                            // 就把该地址存放到集合中
                                            registryList.add(item.getRegistryValue());
                                        }
                                        // 把集合添加到Map中，至此，一个appName对应的执行器地址，这样的数据就缓存成功了
                                        appAddressMap.put(appname, registryList);
                                    }
                                }
                            }

                            // 到这里会遍历最开始查询出来的自动注册的所有执行器组，
                            // 【注意】这时候，在上面的那个循环中，已经把所有未过期的执行器的信息用键值对的方式缓存在Map中了
                            for (XxlJobGroup group : groupList) {
                                // 根据这个执行器注册到注册中心时记录的appName
                                // 从Map中查询到所有的执行器地址，是个集合
                                List<String> registryList = appAddressMap.get(group.getAppname());
                                String addressListStr = null;
                                if (registryList != null && !registryList.isEmpty()) {
                                    // 如果执行器地址不为空，就把地址排一下序
                                    // 这里排序有什么意义呢？暂时没想到，因为是路由策略帮我们选择执行器地址的
                                    Collections.sort(registryList);
                                    // 把地址进行拼接
                                    StringBuilder addressListSB = new StringBuilder();
                                    for (String item : registryList) {
                                        addressListSB.append(item).append(",");
                                    }
                                    addressListStr = addressListSB.toString();
                                    // 去掉最后一个逗号
                                    addressListStr = addressListStr.substring(0, addressListStr.length() - 1);
                                }
                                // 然后把最新的执行器地址存放到执行器组中
                                group.setAddressList(addressListStr);
                                // 更新执行器组的更新时间
                                group.setUpdateTime(new Date());
                                // 在数据库中落实执行器组的更新
                                // 到这里，大家应该能意识到了，执行器把自己注册到调度中心是通过XxlJobRegistry对象来
                                // 封装注册信息的，会被记录到数据库中
                                // 但是注册线程会在后台默默工作，把各个appName相同的执行器的地址整合到一起，用XxlJobGroup
                                // 对象封装，等待调度定时任务的时候，其实就是从XxlJobGroup对象中获得appName的所有执行器地址，
                                // 然后根据路由策略去选择具体的执行器地址来远程调用，这就是和注册有关的所有逻辑了
                                XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().update(group);
                            }
                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
                        }
                    }
                    try {
                        // 线程在这里睡30s，也就意味着检测周期为30s
                        TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                    } catch (InterruptedException e) {
                        if (!toStop) {
                            logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, job registry monitor thread stop");
            }
        });
        registryMonitorThread.setDaemon(true);
        registryMonitorThread.setName("xxl-job, admin JobRegistryMonitorHelper-registryMonitorThread");
        registryMonitorThread.start();
    }

    /**
     * 关闭线程池的方法
     */
    public void toStop() {
        toStop = true;
        registryOrRemoveThreadPool.shutdownNow();
        registryMonitorThread.interrupt();
        try {
            registryMonitorThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }


    // ---------------------- helper ----------------------

    /**
     * 注册执行器的方法
     */
    public ReturnT<String> registry(RegistryParam registryParam) {
        // 参数校验
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }
        // 提交注册执行器的任务给线程池执行
        registryOrRemoveThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                // 这里的意思很简单，就是先根据registryParam的参数去数据库中更新相应的数据
                // 如果返回的是0，说明数据库中没有相应的信息，该执行器还没注册到注册中心，
                // 所以下面就可以直接新增这一条数据即可
                int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao()
                        .registryUpdate(registryParam.getRegistryGroup(), registryParam.getRegistryKey(),
								registryParam.getRegistryValue(), new Date());
                if (ret < 1) {
                    // 这里就是数据库中没有相应的数据，直接新增即可
                    XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao()
                            .registrySave(registryParam.getRegistryGroup(), registryParam.getRegistryKey(),
									registryParam.getRegistryValue(), new Date());

                    // 该方法从名字上看是刷新注册表信息的意思，但是暂时还没实现
                    freshGroupRegistryInfo(registryParam);
                }
            }
        });
        return ReturnT.SUCCESS;
    }

    /**
     * 移除过期的执行器地址
     */
    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        // 参数校验
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }
        // 将任务提交给线程池来处理
        registryOrRemoveThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                // 在这里直接根据registryParam从数据库中删除对应的执行器地址
                // 这里的返回结果是删除了几条数据的意思
                int ret = XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao()
                        .registryDelete(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue());
                if (ret > 0) {
                    // 该方法从名字上看是刷新注册表信息的意思，但是暂时还没实现
                    freshGroupRegistryInfo(registryParam);
                }
            }
        });
        return ReturnT.SUCCESS;
    }

    /**
     * 这个方法是空的，可能作者也没想好怎么去实现
     */
    private void freshGroupRegistryInfo(RegistryParam registryParam) {
        // Under consideration, prevent affecting core tables
    }

}
