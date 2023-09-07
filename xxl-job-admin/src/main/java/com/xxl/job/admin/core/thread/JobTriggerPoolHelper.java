package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.trigger.XxlJobTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KEYPOINT 快慢线程池
 * <h1>
 *     xxl-job 服务器的重点类，在这个类中初始化了两个线程池，一个快、一个慢，要被执行的任务会被包装成触发器任务，
 *     提交给这两个线程池中的一个，然后由线程池去执行触发器的任务，在任务中会进行远程调用
 * </h1>
 *
 * @author xuxueli 2018-07-03 21:08:07
 */
public class JobTriggerPoolHelper {
    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);


    // ---------------------- trigger pool ----------------------

    /*
        下面这两个快慢线程池没有什么本质上的区别，都是线程池而已，只不过快线程池的最大线程数为 200，
        慢线程池的最大线程数为 100，任务队列也是如此，并且会根据任务执行的耗时来决定下次任务执行的时候
        是要让快线程池来执行还是让慢线程池来执行，默认选择的是使用快线程池来执行

        【注意】所谓的快慢线程池并不是说线程执行任务的快慢，而是任务的快慢决定了线程的快慢
         直接来讲，执行耗时较短的任务，我们可以称它为快速任务，而执行这些任务的线程池，就被称为了
         快线程池，如果任务耗时较长，就给慢线程池来执行
     */

    /**
     * 快线程池
     */
    private ThreadPoolExecutor fastTriggerPool = null;
    /**
     * 慢线程池
     */
    private ThreadPoolExecutor slowTriggerPool = null;

    /**
     * <h2>在这里，创建了两个快慢线程池</h2>
     */
    public void start(){
        // 快线程池，默认最大线程数为 200
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(1000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode());
                    }
                });

        // 慢线程池，默认最大线程数为 100
        slowTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode());
                    }
                });
    }

    /**
     * <h2>关闭线程池</h2>
     */
    public void stop() {
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        logger.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
    }


    /**
     * 获取当前的系统时间，这里计算出来的其实是系统当前的分钟数
     */
    private volatile long minTim = System.currentTimeMillis()/60000;
    /**
     * 如果有任务出现慢执行情况了，就会被记录在该 Map 中
     * 所谓慢执行，就是执行的时间超过了 500ms，该 Map 的 key 为 job 的 id，value 为慢执行的次数
     * 如果一分钟慢执行的次数超过了 10 次，该任务就会被交给慢线程池来执行
     * 而该 Map 也会一分钟清空一次，来循环记录慢执行的情况
     */
    private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();


    /**
     * <h2>
     *     这个方法就是远程调用的七点，很重要的入口方法，JobInfoController 中的 triggerJob 会调用到这里，
     *     还有 JobScheduleHelper 类中也会调用到该方法。当然，在该方法外面还有一层 trigger 方法，这个方法
     *     就在本类中，属于是该方法的外层方法
     * </h2>
     *
     * @param jobId 任务ID
     * @param triggerType 任务触发的枚举类型（如 手动触发、手动调用该任务、执行一次）
     * @param failRetryCount 失败重试次数
     * @param executorShardingParam 分片参数
     * @param executorParam 执行器方法参数
     * @param addressList 执行器的地址列表
     */
    public void addTrigger(final int jobId,
                           final TriggerTypeEnum triggerType,
                           final int failRetryCount,
                           final String executorShardingParam,
                           final String executorParam,
                           final String addressList) {

        // 默认使用快线程池
        ThreadPoolExecutor triggerPool_ = fastTriggerPool;
        // 用任务 ID 从慢执行 Map 中得到该 job 对应的慢执行次数
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        // 如果慢执行次数不为 null，并且一分钟超过了 10，就选用慢线程池来执行该任务
        if (jobTimeoutCount!=null && jobTimeoutCount.get() > 10) {
            // 这里选用慢线程池了
            triggerPool_ = slowTriggerPool;
        }

        // 在这里就把任务提交给了线程池，在这个任务执行一个触发器任务，把刚才传进来的 job 的各种信息整合到一起
        // 在触发器任务中，会进行 job 的远程调用，这个调用链还是比较短的，执行流程也很清晰
        triggerPool_.execute(new Runnable() {
            @Override
            public void run() {
                // 再次获取当前时间，这个时间后面会用到
                long start = System.currentTimeMillis();
                try {
                    // KEYPOINT
                    // 触发器任务开始执行了，在该方法内部会进行远程调用
                    XxlJobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                } finally {
                    // 这里再次获取当前的分钟数，这个分钟数会和刚才上面得到的那个分钟数做对比
                    long minTim_now = System.currentTimeMillis()/60000;
                    // 这里就用到了两个分钟数做对比，如果两个分钟数不等，说明过去了一分钟
                    // 而慢执行 Map 中的数据是一分钟清理一次，所以这里就把慢执行 Map 清空掉
                    // 【注意】这个清空的动作是线程池中的线程来执行的，并且这个动作是在 finally 代码块中执行的
                    // 也就是意味着是在上面的触发器任务执行完毕后才进行清空操作
                    if (minTim != minTim_now) {
                        minTim = minTim_now;
                        jobTimeoutCountMap.clear();
                    }

                    // 在这里用当前毫秒值减去之前得到的毫秒值
                    long cost = System.currentTimeMillis()-start;
                    // 判断任务执行时间是否超过 500ms
                    // 这里仍然要结合上面的 finally 代码块来理解，因为触发器任务执行完了才会执行 finally 代码块
                    // 中的代码，所以这个时候也就能用得到 job 的执行时间了
                    if (cost > 500) {
                        // 超过 500 毫秒了，就判断当前执行的任务为慢执行任务，所以将它在慢执行 Map 中记录一次
                        // Map 的 key 为 jobId，value 为慢执行的次数
                        AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                        if (timeoutCount != null) {
                            // 慢执行的次数加一
                            timeoutCount.incrementAndGet();
                        }
                    }

                }

            }
        });
    }



    // ---------------------- helper ----------------------

    private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

    public static void toStart() {
        helper.start();
    }
    public static void toStop() {
        helper.stop();
    }

    /**
     * @param jobId
     * @param triggerType
     * @param failRetryCount
     * 			>=0: use this param
     * 			<0: use param from job info config
     * @param executorShardingParam
     * @param executorParam
     *          null: use job param
     *          not null: cover job param
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam, String addressList) {
        helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }

}
