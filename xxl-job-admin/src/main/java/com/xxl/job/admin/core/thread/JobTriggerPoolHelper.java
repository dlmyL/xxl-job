package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.trigger.XxlJobTrigger;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 触发任务的类，这个类就负责把定时任务的信息向程序内部继续传递下去。
 * xxl-job服务器的重点类，在这个类中初始化了两个线程池，一个快、一个慢，
 * 要被执行的任务会被包装成触发器任务，提交给这两个线程池中的一个，然后
 * 由线程池去执行触发器的任务，在任务中会进行远程调用。
 */
@Slf4j
public class JobTriggerPoolHelper {

    // === trigger pool ===

    /*
    为什么会有快慢两种线程池呢？
    主要是为了做一个线程池的隔离，将正常执行的任务放入到fastTriggerPool中，
    将执行偏慢的任务放到slowTriggerPool中，避免执行较慢的任务占用过多资源，
    影响到了其他正常任务的调度。

    什么样的任务算作是慢任务？
    在程序中使用ConcurrentHashMap维护了一个计数器，key为jobId，value为超
    时次数，当任务触发时间超过500ms时，超时次数 + 1，同一个任务在1分钟内超时
    超过了10次，这个任务就会被定义为慢任务，后续就会由slowTriggerPool来进行
    调度。后续通过helper.addTrigger，就会从线程池中获取一个线程，执行任务触
    发操作，在做实际的触发操作之前，还需要处理一下传入的参数。

    下面这两个快慢线程池没有什么本质上的区别，都是线程池而已，只不过快线程池的
    最大线程数为200，慢线程池的最大线程数为 100，任务队列也是如此，并且会根据
    任务执行的耗时来决定下次任务执行的时候是要让快线程池来执行还是让慢线程池来
    执行，默认选择的是使用快线程池来执行。
    注意：所谓的快慢线程池并不是说线程执行任务的快慢，而是任务的快慢决定了线程
    的快慢直接来讲，执行耗时较短的任务，我们可以称它为快速任务，而执行这些任务
    的线程池，就被称为了快线程池，如果任务耗时较长，就给慢线程池来执行。
    */
    private ThreadPoolExecutor fastTriggerPool = null;
    private ThreadPoolExecutor slowTriggerPool = null;

    public void start() {
        // 快线程池，核心线程10，最大线程200，阻塞队列1000
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                r -> new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode())
        );
        // 慢线程池，核心线程10，最大线程100，阻塞队列2000
        slowTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(2000),
                r -> new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode())
        );
    }

    public void stop() {
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        log.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
    }

    /**
     * 获取当前的系统时间，这里计算出来的其实是系统当前的分钟数，下面马上就会用到
     */
    private volatile long minTim = System.currentTimeMillis() / 60000;

    /**
     * 如果有任务出现慢执行情况了，就会被记录在该Map中。
     * 所谓慢执行，就是执行的时间超过了500ms，该Map的key为jobId，value
     * 为慢执行的次数，如果一分钟慢执行的次数超过了10次，该任务就会被交给
     * 慢线程池来执行，而该Map也会一分钟清空一次，来循环记录慢执行的情况。
     * | 任务id | 慢执行次数 | 线程池执行 |
     * |   1    |    15    | 慢线程池  |
     * |   2    |    5     | 快线程池  |
     */
    private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();

    /**
     * 这个方法就是远程调用的起点，很重要的入口方法，JobInfoController中的triggerJob会调用到这里，
     * 还有JobScheduleHelper类中也会调用到该方法。当然，在该方法外面还有一层trigger方法，这个方法
     * 就在本类中，属于是该方法的外层方法
     *
     * @param jobId                 任务ID
     * @param triggerType           任务触发的枚举类型（如 手动触发、手动调用该任务、执行一次）
     * @param failRetryCount        失败重试次数
     * @param executorShardingParam 分片参数
     * @param executorParam         执行器方法参数
     * @param addressList           执行器的地址列表
     */
    public void addTrigger(final int jobId,
                           final TriggerTypeEnum triggerType,
                           final int failRetryCount,
                           final String executorShardingParam,
                           final String executorParam,
                           final String addressList) {
        /*
        ==确定执行任务的线程池==
        快慢线程执行，默认是快线程，如果任务是在1分钟内超时10次，转换到慢线程执行。
        用jobId从jobTimeoutCountMap中得到该job对应的慢执行次数，如果慢执行次数
        不为null，并且一分钟超过了10次，就选用慢线程池来执行该任务。
         */
        ThreadPoolExecutor triggerPool_ = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {
            triggerPool_ = slowTriggerPool;
        }

        /*
        ==向线程池中提交任务==
        在这里就把任务提交给了线程池，在这个任务执行一个触发器任务，把刚才传进来的job
        的各种信息整合到一起，在触发器任务中，会进行job的远程调用，这个调用链还是比较
        短的，执行流程也很清晰。
         */
        triggerPool_.execute(() -> {
            // 再次获取当前时间，这个时间后面会用到
            long start = System.currentTimeMillis();
            try {
                // kのt { 触发任务 }
                XxlJobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                // 这里再次获取当前的分钟数，这个分钟数会和minTim做对比
                long minTim_now = System.currentTimeMillis() / 60000;
                /*
                这里就用到了两个分钟数做对比，如果两个分钟数不等，说明过去了一分钟，
                而慢执行Map中的数据是一分钟清理一次，所以这里就把慢执行Map清空掉。
                注意：这个清空的动作是线程池中的线程来执行的，并且这个动作是在finally
                代码块中执行的，也就是意味着是在上面的触发器任务执行完毕后才进行清空操作。
                 */
                if (minTim != minTim_now) {
                    minTim = minTim_now;
                    jobTimeoutCountMap.clear();
                }

                // 在这里用当前毫秒值减去之前得到的毫秒值，得到任务执行耗时
                long cost = System.currentTimeMillis() - start;
                /*
                判断任务执行时间是否超过500ms。
                这里仍然要结合上面的finally代码块来理解，因为远程调用行完了才会执行finally
                代码块中的代码，所以这个时候也就能用得到job的执行时间。
                执行时间超过500毫秒了，就判断当前执行的任务为慢执行任务，所以将它在慢执行Map
                中记录一次，Map的key为jobId，value为慢执行的次数。
                 */
                if (cost > 500) {
                    AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                    if (timeoutCount != null) {
                        timeoutCount.incrementAndGet();
                    }
                }
            }
        });
    }


    // === helper ===

    /**
     * 静态成员变量，说明该变量也只会初始化一次，而且不会
     * 直接对外暴露，而是通过下面的两个方法间接在外部调用。
     */
    private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

    public static void toStart() {
        helper.start();
    }

    public static void toStop() {
        helper.stop();
    }

    /**
     * 该方法会对外暴露，然后调用到该类内部的addTrigger方法，该方法的作用就是把要执行的
     * job包装成一个触发器任务，在触发器任务中进行远程调用，然后在执行器那一端执行该job。
     *
     * @param jobId                 定时任务配置id，用于查询定时任务配置
     * @param triggerType           定时任务的触发类型，用于记录日志（如 手动触发、手动调用该任务、执行一次）
     * @param failRetryCount        定时任务的失败重试次数，这里传入-1，表示使用定时任务配置中的重试次数
     * @param executorShardingParam 定时任务的分片参数，对应Web界面的“路由策略-分片广播”，并且重试次数大于0
     * @param executorParam         执行器方法参数，对应Web界面的”任务参数“
     * @param addressList           执行器的地址列表，对应Web界面的“机器地址”
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount,
                               String executorShardingParam, String executorParam, String addressList) {
        helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }
}
