package com.xxl.job.core.thread;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import com.xxl.job.core.executor.XxlJobExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * <h1>执行器一端进行远程注册的类，将执行器注册到调度中心</h1>
 */
@Slf4j
public class ExecutorRegistryThread {

    private static ExecutorRegistryThread instance = new ExecutorRegistryThread();

    public static ExecutorRegistryThread getInstance() {
        return instance;
    }

    /**
     * 将执行器注册到调度中心的线程，也是真正干活的线程
     */
    private Thread registryThread;
    /**
     * 线程是否停止工作的标记
     */
    private volatile boolean toStop = false;

    /**
     * <h2>启动注册线程</h2>
     */
    public void start(final String appname, final String address) {
        // 对 appName 判空，这个就是执行器要记录在调度中心的名称
        if (appname == null || appname.trim().length() == 0) {
            log.warn(">>>>>>>>>>> xxl-job, executor registry config fail, appname is null.");
            return;
        }
        // 判断 adminBizList 集合不为空，因为个客户端是用来和调度中心通信的
        if (XxlJobExecutor.getAdminBizList() == null) {
            log.warn(">>>>>>>>>>> xxl-job, executor registry config fail, adminAddresses is null.");
            return;
        }

        // 创建线程
        registryThread = new Thread(new Runnable() {
            @Override
            public void run() {
                // 在一个循环中执行注册任务
                while (!toStop) {
                    try {
                        //根据 appName 和 address 创建注册参数，注意，这里的 address 是执行器的地址，只有一个，别和调度中心的地址搞混了
                        RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistType.EXECUTOR.name(), appname, address);
                        // 这里考虑到调度中心也许是以集群的形式存在，所以从集合中得到每一个和调度中心通话地客户端，然后发送注册消息即可
                        for (AdminBiz adminBiz : XxlJobExecutor.getAdminBizList()) {
                            try {
                                // EXEC => {调度中心根地址}/api/registry
                                // 在这里执行注册
                                ReturnT<String> registryResult = adminBiz.registry(registryParam);
                                if (registryResult != null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
                                    registryResult = ReturnT.SUCCESS;
                                    log.debug(">>>>>>>>>>> xxl-job registry success, registryParam:{}, " + "registryResult:{}", new Object[]{registryParam, registryResult});
                                    // 注册成功则打破循环，因为注册成功一个后，调度中心就把相应的数据写到数据库中了，
                                    // 没必要每个都注册，直接退出循环即可，注册不成功，再找下一个注册中心继续注册
                                    break;
                                } else {
                                    // 如果注册失败了，就寻找下一个调度中心继续注册
                                    log.info(">>>>>>>>>>> xxl-job registry fail, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
                                }
                            } catch (Exception e) {
                                log.info(">>>>>>>>>>> xxl-job registry error, registryParam:{}", registryParam, e);
                            }
                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            log.error(e.getMessage(), e);
                        }
                    }

                    try {
                        if (!toStop) {
                            /*
                             执行器注册到调度中心的心跳时间，其实就是 30s 重新注册一次，刷新注册时间，
                             以防止调度中心主观任务执行器下线了
                             RegistryConfig.BEAT_TIMEOUT=30

                             这里是每间隔 30s，就再循环重新注册一次，也就是【维持心跳信息】
                            */
                            TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                        }
                    } catch (InterruptedException e) {
                        if (!toStop) {
                            log.warn(">>>>>>>>>>> xxl-job, executor registry thread interrupted, error msg:{}", e.getMessage());
                        }
                    }
                }

                // registry remove
                try {
                    /*
                    这里要注意，当程序执行到这里的时候，就意味着跳出了上面那个工作线程的循环，其实也就意味着那个工作线程要结束工作了，不再注册执行器，
                    也不再刷新心跳信息，这也就意味着执行器这一端可能不再继续提供服务了，所以下面要把注册的执行器信息从调度中心删除，所以发送删除的信息
                    给调度中心
                     */
                    // 再次创建注册参数对象
                    RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistType.EXECUTOR.name(), appname, address);
                    for (AdminBiz adminBiz : XxlJobExecutor.getAdminBizList()) {
                        try {
                            // EXEC => {调度中心根地址}/api/registryRemove
                            // 在这里发送删除执行器的信息
                            ReturnT<String> registryResult = adminBiz.registryRemove(registryParam);
                            if (registryResult != null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
                                registryResult = ReturnT.SUCCESS;
                                log.info(">>>>>>>>>>> xxl-job registry-remove success, registryParam:{}, " + "registryResult:{}", new Object[]{registryParam, registryResult});
                                break;
                            } else {
                                log.info(">>>>>>>>>>> xxl-job registry-remove fail, registryParam:{}, " + "registryResult:{}", new Object[]{registryParam, registryResult});
                            }
                        } catch (Exception e) {
                            if (!toStop) {
                                log.info(">>>>>>>>>>> xxl-job registry-remove error, registryParam:{}", registryParam, e);
                            }
                        }
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        log.error(e.getMessage(), e);
                    }
                }
                log.info(">>>>>>>>>>> xxl-job, executor registry thread destroy.");
            }
        });
        // 在这里创建线程
        registryThread.setDaemon(true);
        registryThread.setName("xxl-job, executor ExecutorRegistryThread");
        registryThread.start();
    }

    /**
     * <h2>终止注册线程的方法</h2>
     */
    public void toStop() {
        // 改变线程是否停止的标记
        toStop = true;
        if (registryThread != null) {
            // 中断注册线程
            registryThread.interrupt();
            try {
                // 在在哪个线程中调用了注册线程的join方法，哪个线程就会暂时阻塞住，等待注册线程执行完了才会继续向下执行
                registryThread.join();
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }

    }

}
