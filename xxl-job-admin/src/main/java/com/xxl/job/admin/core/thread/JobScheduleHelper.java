package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.scheduler.MisfireStrategyEnum;
import com.xxl.job.admin.core.scheduler.ScheduleTypeEnum;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 该类完全是xxl-job服务端最核心的类了，任务的调度就是在这个类中执行的，线程会不停的
 * 扫描数据库，看哪些任务该执行了，执行完毕后，还要计算下一次执行的时间。
 */
@Slf4j
public class JobScheduleHelper {

    @Getter
    private static JobScheduleHelper instance = new JobScheduleHelper();

    /**
     * 这里定义了5000毫秒，查询数据库的时候会用到，
     * 查询就是当前时间5秒之内可以执行的定时任务信息。
     */
    public static final long PRE_READ_MS = 5000;

    /**
     * 【任务扫描线程】
     * 下面这个成员变量就是用来扫描任务的线程，其实在该类中工作的都是线程，并没有创建线
     * 程池。这个线程会在一个循环中不停的扫描数据库，看哪些任务该执行了，哪些任务已经过
     * 了执行时间，然后进行相应的处理这个线程也会提交给触发器线程池执行触发器任务，但是
     * 这个并不是该线程的主要工作，该线程的主要工作是扫描数据库，查询到期的执行任务，并
     * 且更新任务的下一次执行时间。
     */
    private Thread scheduleThread;
    private volatile boolean scheduleThreadToStop = false;

    /**
     * 【时间轮线程】
     * 这个就是时间轮线程，时间轮并不只是线程，也并不只是容器，容器和线程结合在一起，构
     * 成了可以运行的时间轮，这个时间轮线程就是用来主要向触发器线程池提交触发任务的，它
     * 提交的任务是从Map中获得的，而Map中的任务是由上面的任务扫描线程添加的。
     */
    private Thread ringThread;
    private volatile boolean ringThreadToStop = false;

    /**
     * 【时间轮容器】
     * 这个就是时间轮的容器，该容器中的数据是由任务扫描线程添加的，但是移除是由时间轮线程
     * 线程移除的，Map的key为时间轮中任务的执行时间，value是需要执行的定时任务ID集合，意
     * 思就是在这个时间，有这么多定时任务要被提交给调度线程池。
     */
    private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();

    /**
     * 当该方法被调用的时候，调度模块中调度定时任务的线程就开始工作了。
     * 该线程会在一个循环中不停地扫描数据库中所有的定时任务信息，然后判断每个定时任务是否到
     * 了执行的时间，如果到达执行的时间，就让线程去远程通知定时任务程序执行定时任务。把扫描
     * 数据库查询定时任务信息和调度任务给触发线程池分离，这样就可以让一个线程专注地从数据库
     * 中查询定时任务信息，把查询到的可以执行的定时任务缓存到Map中，然后另一个ringThread线
     * 程只负责调度任务，把任务提交给触发线程池。这两个线程都可以专注地干自己的工作，并且在
     * 很大程度上实现了定时任务触发的精准程度。
     * <p>
     * 解决调度中心的性能问题：引入了JobTriggerPoolHelper中的线程池，让调度中心的调度定时
     * 任务的线程，也就是不停扫描数据库，并判断哪些定时任务是否可以执行了的scheduleThread
     * 线程，和真正去远程通知定时任务执行程序执行定时任务的工作彻底分开，scheduleThread线程
     * 只负责扫描数据库，把可以执行的定时任务信息提交给JobTriggerPoolHelper中的线程池，然后
     * 维护定时任务下一次的执行时间。
     */
    public void start() {
        // ====== 任务扫描线程 ======
        scheduleThread = new Thread(new Runnable() {
            // kのt{【性能问题】抢分布式锁，一直扫表}
            @Override
            public void run() {
                try {
                    /*
                    ==时间对齐==
                    这里的逻辑非常简单，就是起到了一个对齐时间的效果，因为这个调度线程的调度周期
                    是5秒，所以如果现在的时候不是整数秒的话，就让调度线程睡到整秒数再启动。比如
                    System.currentTimeMillis()%1000计算得到的时间是得到了一个300ms的值，那么
                    5000ms-300ms，就是让线程睡4700ms，就会到达一个整数秒了，然后直接启动就行。
                     */
                    TimeUnit.MILLISECONDS.sleep(5000 - (System.currentTimeMillis() % 1000));
                } catch (InterruptedException e) {
                    if (!scheduleThreadToStop) {
                        log.error(e.getMessage(), e);
                    }
                }

                log.info(">>>>>>>>> init xxl-job admin scheduler success.");

                /*
                【注意】源码中写死的每秒最大可调度任务数为【6000】，int preReadCount = (200 + 100) * 20。
                这里就是作者写死的一个每秒可以调度的任务量，这里默认每个定时任务执行大概耗时50ms，1s=1000ms，
                所以1秒钟可以执行20个定时任务，但是执行定时任务，在调度中心实际上就是用快慢线程池执行触发器任
                务，因为定时任务真正执行还是在执行器那一端，所以，在加上快慢线程池拥有的线程总数，假如快慢线程
                池都拉满了，都到达了最大线程数，那就是200+100，总的线程数为300，每秒每个线程可以执行20个定时
                任务，现在有300个线程，所以，最后可以得到，每秒最多可以调度6000个定时任务，6000就是一个限制的
                最大值，如果调度线程要扫描数据库，从数据库取出要执行的任务，每次最多可以取6000个，数据库取出任
                务限制6000，也只是为了配合这个限制的数量。
                 */
                int preReadCount = (XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax() +
                        XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax()) * 20;

                // 如果调度线程没有停止，将会被一直执行
                while (!scheduleThreadToStop) {
                    // 这个时间用来做判断的，看看扫描数据库耗费了多少时间
                    long start = System.currentTimeMillis();

                    /*
                    判断是否从数据库中读取到了数据，读取到了就意味着有任务要执行，
                    当有查询出了可调度的定时任务时，值为true，反之为false。
                     */
                    boolean preReadSuc = true;

                    /*
                    下面这几个步骤都和数据库有关，因为xxl-job是使用数据库来实现分布式锁的，
                    既然是数据锁，就不能自动提交事务，所以这里要手动设置
                     */
                    Connection conn = null;
                    // 设置事务不自动提交
                    Boolean connAutoCommit = null;
                    PreparedStatement preparedStatement = null;
                    try {
                        // 获得数据库连接
                        conn = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
                        // 拿到事务自动提交标志
                        connAutoCommit = conn.getAutoCommit();
                        // 设置为不自动提交
                        conn.setAutoCommit(false);
                        /*
                        设置sql语句，获得【数据库锁】
                        【注意】一旦调度中心形成集群，就要防止定时任务被重复调度，这时就
                        必须要用到【分布式锁】，在xxl-job 中，分布式锁是用数据库实现的。
                         */
                        preparedStatement = conn.prepareStatement("select * from xxl_job_lock where lock_name = 'schedule_lock' for update");
                        // 开始执行sql语句，拿到数据库锁(分布式锁)
                        preparedStatement.execute();

                        /*
                        获取当前时间，这里要把这个时间和上面那个start做一下区分，
                        这两个时间变量的作用不同，现在这个时间变量是用来得到要调
                        度的任务的，上面那个是最后用来做判断的，看看扫描数据库耗
                        费了多少时间。
                         */
                        long nowTime = System.currentTimeMillis();
                        /*
                        查找出当前时间+5秒之内所有可以执行的定时任务信息：
                            SELECT *
                            FROM  xxl_job_info AS t
                            WHERE t.trigger_status = 1
                            AND   t.trigger_next_time <= #{maxNextTime} // 预读时间，当前时间+5s
                            ORDER BY id ASC
                            LIMIT #{pagesize} // 预读数量，默认6000
                         */
                        List<XxlJobInfo> scheduleList = XxlJobAdminConfig.getAdminConfig()
                                .getXxlJobInfoDao().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);
                        // 如果在数据库中查询到了要执行的任务，就循环的处理每一个任务
                        if (!CollectionUtils.isEmpty(scheduleList)) {
                            // 时间轮入轮操作
                            for (XxlJobInfo jobInfo : scheduleList) {
                                /*
                                这里做了一个判断：刚才得到的当前时间 > 任务的下一次执行时间 + 5秒，为什么会出现这种情况呢？

                                仔细想想，本来一个任务被调度执行了，就会计算出它下一次的执行时间，然后更新数据库中的任务的
                                下一次执行时间，但是如果服务器宕机了呢？
                                本来上一次要执行的任务却没有执行，比如这个任务要在第5秒执行，但是服务器在第4秒就宕机了，重新
                                恢复运行后，已经是第12秒了，现在去数据库中查询任务，12>5+5，就是if括号中的不等式，这样一来，
                                是不是就查到了执行时间比当前时间还小的任务呢？
                                所以，作者早已考虑到了这种情况
                                 */
                                if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS/*已超时5s以上*/) {
                                    log.warn(">>>>>>>>>>> xxl-job, schedule misfire, jobId = " + jobInfo.getId());
                                    /*
                                    既然有过期的任务，就要看看怎么处理，是直接不处理，还是其他的处理方式，这里程序默认的是什么
                                    也不做，过期就过期呗。当然，下面还是要再判断一次，万一失败策略是【立刻重试一次】，那就立刻
                                    执行一次任务的调度。
                                     */
                                    MisfireStrategyEnum misfireStrategyEnum = MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), MisfireStrategyEnum.DO_NOTHING);
                                    if (MisfireStrategyEnum.FIRE_ONCE_NOW == misfireStrategyEnum) {
                                        // ==立即触发任务调度==
                                        JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.MISFIRE, -1,null, null, null);
                                        log.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId());
                                    }

                                    // 在这里把过期任务的下次执行时间刷新一下，放到下一次来执行
                                    refreshNextValidTime(jobInfo, new Date());
                                } else if (nowTime > jobInfo.getTriggerNextTime()/*已超时但不足5s*/) {
                                    /*
                                    这里得到的就是要执行的任务的下一次执行时间同样也小于了当前时间，但是这里和上面不同的是，没有
                                    超过当前时间加5秒那个时间。
                                    现在大家应该都清楚了，上面加的那个5秒其实是调度周期，每一次处理的任务都是当前任务加5秒这个时
                                    间段内的这一次得到的任务仅仅是小于当前时间，但是并没有加上5秒，说明这个任务虽然过期了但仍然是
                                    在当前的调度周期中。比如说这个任务要在第2秒执行，但是服务器在第1秒就宕机了，恢复之后已经是第
                                    4秒了，现在任务的执行时间小于了当前时间，但是仍然在5秒的调度器内，所以直接执行即可。
                                     */

                                    // ==立即触发任务调度==
                                    JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1,
                                            null, null, null);
                                    // 刷新任务下一次的触发时间
                                    refreshNextValidTime(jobInfo, new Date());

                                    log.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId());

                                    /*
                                    如果发现任务在5秒内会再次触发，便会将这个任务直接放入到时间轮中，由时间轮来进行下
                                    一次调度。
                                    下面这个分支中的任务就是比较正常的，但是又有些特殊？
                                    首先判断它是不是在启动的状态，然后判断这个任务的下一次执行时间是否小于这个执行周期。
                                    注意：上面的refreshNextValidTime方法已经把该任务的下一次执行时间更新了，如果更新
                                    后的时间仍然小于执行周期，说明这个任务会在执行周期中再执行一次，当然也可能执行多次。
                                    这个时候，就不让调度线程来处理这个任务了，而是把它提交给时间轮，让时间轮去执行。

                                    【问题思考】为什么需要时间轮去执行呢？
                                               调度线程自己去把任务给触发器线程池执行不行吗？
                                               为什么要设计一个5秒的调度周期呢？
                                               xxl-job定时任务的调度精度究竟准确吗？
                                     */
                                    if (jobInfo.getTriggerStatus() == 1
                                            && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {
                                        /*
                                        计算该任务要放在时间轮的刻度，也就是在时间轮中的执行时间。
                                        【注意】千万别被这个取余迷惑了？
                                        这里的余数计算结果为0~59，单位是秒，意味着时间轮有60个刻度，一个代表一秒。
                                        调度线程是按调度周期来处理任务的，举个例子，调度线程从0秒开始启动，第5秒为一个周期，把
                                        这5秒要执行的任务交给时间轮了，就去处理下一个调度周期，千万不要把调度线程处理调度任务时
                                        不断增加的调度周期就是增长的时间，调度线程每次扫描数据库不会耗费那么多时间，这个时间是
                                        作者自己设定的，并且调度线程也不是真的只按整数5秒去调度任务。实际上，调度线程从0秒开始
                                        工作，扫描0~5秒的任务，调度这些任务耗费了1秒，再次循环时，调度线程就会从1秒开始，处理
                                        1~6秒的任务，虽说是1~6秒，但是1~5秒的任务都被处理过了，但是请大家想一想，有些任务也仅
                                        仅只是被执行了一次，如果有一个任务在0~5秒调度器内被执行了，但是该任务没1秒执行一次，从
                                        第1秒开始，那它是不是会在调度期内执行多次？可是上一次循环它可能最多只被执行了2次，一次
                                        在调度线程内，一次在时间轮内，还有几次并未执行呢？所以要交给下一个周期去执行，但是这时
                                        候它的下次执行时间还在当前时间的5秒内，如果下个周期直接从6秒开始，这个任务就无法执行了，
                                        大家可以仔细想想。【时间轮才是真正按照时间增长的速度去处理定时任务的】
                                         */
                                        // 计算该任务要放在时间轮的刻度，也就是在时间轮中的执行时间
                                        int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);
                                        // ==把任务推送到时间轮==
                                        pushTimeRing(ringSecond, jobInfo.getId());
                                        /*
                                        刷新定时任务的下一次执行时间。
                                        注意：这里传进来的不是当前时间了，而是定时任务的下一次执行时间，
                                        因为放到时间轮中就意味着它要执行了，所以计算新的执行时间就行了。
                                         */
                                        refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                                    }
                                } else /*还未到触发时间*/{
                                    /*
                                    最后这里得到的就是最正常的任务，也就是执行时间在当前时间之后，但是又小于执行周期的
                                    任务，上面的几个判断，都是当前时间大于任务的下次执行时间，实际上都是在过期的任务中
                                    做判断。这样的任务就很好处理了，反正都是调度周期，也就是当前时间5秒内要执行的任务，
                                    所以直接放到时间轮中就行。计算出定时任务在时间轮中的刻度，其实就是定时任务执行的时
                                    间对应的秒数，随着时间的流逝，时间轮也是根据当前时间秒数来获取要执行的任务的，所以
                                    这样就可以对应上了。
                                     */
                                    // 计算该任务要放在时间轮的刻度，也就是在时间轮中的执行时间
                                    int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);
                                    // ==把任务推送到时间轮==
                                    pushTimeRing(ringSecond, jobInfo.getId());
                                    // 刷新定时任务下一次的执行时间
                                    refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                                }
                            }

                            /*
                            最后更新一下所有的任务执行信息：
                                UPDATE xxl_job_info
                                SET trigger_last_time = #{triggerLastTime}, 任务最后一次触发时间
                                    trigger_next_time = #{triggerNextTime}, 任务下一次触发时间
                                    trigger_status = #{triggerStatus}       任务状态
                                WHERE id = #{id}                            任务ID
                             */
                            for (XxlJobInfo jobInfo : scheduleList) {
                                XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                            }

                        } else {
                            // 走到这里，说明根本就没有从数据库中扫描到任何任务，把preReadSuc标记设为false
                            preReadSuc = false;
                        }
                    } catch (Exception e) {
                        if (!scheduleThreadToStop) {
                            log.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e.getMessage(), e);
                        }
                    } finally {
                        // 提交事务、释放锁、再次设置非手动提交、释放资源
                        if (conn != null) {
                            try {
                                conn.commit();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.setAutoCommit(connAutoCommit);
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                        }
                        if (null != preparedStatement) {
                            try {
                                preparedStatement.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                        }
                    }

                    /*
                    再次得到当前时间，然后减去开始执行扫描数据库的开始时间，
                    就得到了执行扫描数据库，并且调度任务的总耗时。
                     */
                    long cost = System.currentTimeMillis() - start;
                    /*
                    这里有个判断，1000毫秒就是1秒，如果总耗时小于1s，就默认数据库中可能没多少
                    数据，线程不必工作的这么繁忙，所以下面要让线程休息一会儿，然后再继续工作。
                     */
                    if (cost < 1000) {
                        try {
                            /*
                            下面有一个三元运算，判断preReadSuc是否为true，如果扫描到数据了，
                            就让该线程小睡一会儿，最多睡1秒。如果根本就没有数据，就说明5秒的
                            调度器内没有任何任务可以执行，那就让线程最多睡5秒，把时间睡过去，
                            过5秒再开始工作。
                             */
                            long sleepTime = (preReadSuc ? 1000 : PRE_READ_MS) - (System.currentTimeMillis() % 1000);
                            TimeUnit.MILLISECONDS.sleep(sleepTime);
                        } catch (InterruptedException e) {
                            if (!scheduleThreadToStop) {
                                log.error(e.getMessage(), e);
                            }
                        }
                    }
                }

                log.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
            }
        });
        scheduleThread.setDaemon(true);
        scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();


        // ====== 时间轮线程 ======
        ringThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!ringThreadToStop) {
                    try {
                        /*
                        这里让线程睡一会儿，作用还是比较明确的，因为该线程是时间轮线程，时间轮执行任务是按照时间
                        刻度来执行的，如果这一秒内的所有任务都执行完了，但是耗时只用了500毫秒，剩下的500毫秒就只
                        好睡过去，等待下一个整秒到来再继续开始工作，System.currentTimeMillis() % 1000计算出来
                        的结果如果是500ms，1000-500=500线程就继续睡500毫秒，如果System.currentTimeMillis() %
                        1000计算出来的是0，说明现在是整秒，那就睡1秒，等到下个工作时间再开始工作。
                         */
                        TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                    } catch (InterruptedException e) {
                        if (!ringThreadToStop) {
                            log.error(e.getMessage(), e);
                        }
                    }

                    try {
                        /*
                        即将要执行的定时任务ID集合。
                        时间轮是一个Map容器，Map的key是定时任务要执行的时间，value是定时
                        任务的JobId的集合，到了固定时间，要把对应时刻的定时任务从集合中取
                        出来，所以自然也要用集合来存放这些定时任务的id。
                         */
                        List<Integer> ringItemData = new ArrayList<>();
                        /*
                        ==获取当前时间的秒数==
                        避免处理耗时太长，跨过刻度，向前校验一个刻度
                         */
                        int nowSecond = Calendar.getInstance().get(Calendar.SECOND);
                        /*
                        ==从时间轮中取出当前秒和前一秒的所有任务，循环ringItemData，依次触发其
                        中的每一个任务==
                        下面这里很有意思，如果我们计算出来的是第3秒，时间轮线程会把第2秒和第3秒
                        的任务都取出来，一起执行。这里肯定会让大家感到困惑，时间轮不是按照时间刻
                        度走的吗？如果走到第3秒的刻度，说明第2秒的任务已经执行完了，为什么还要再
                        拿出来？这是因为考虑到定时任务的调度情况了，如果时间轮某个刻度对应的定时
                        任务太多，本来该最多1秒就调度完成的，结果调度了2秒，直接把下一个刻度跳过
                        了，这样不就出错了吗？所以，每次执行的时候要把前一秒的也取出来，检查一下
                        看是否有任务，这也算是一个兜底的方法。
                         */
                        for (int i = 0; i < 2; i++) {
                            /*
                            循环两次，第一次取出当前刻度的任务，第二次取出前一刻度的任务。
                            注意：这里取出任务的时候，定时任务就会从时间轮中被删除。
                             */
                            List<Integer> tmpData = ringData.remove((nowSecond + 60 - i) % 60);
                            if (tmpData != null) {
                                // 把要执行的定时任务ID数据添加到ringItemData集合中
                                ringItemData.addAll(tmpData);
                            }
                        }

                        log.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + Collections.singletonList(ringItemData));

                        /*
                        如果存放要执行的定时任务的ID不为空，就表示里面有任务，
                        那么就先遍历要执行的定时任务，再循环执行，最后清空集合。
                         */
                        if (!ringItemData.isEmpty()) {
                            for (int jobId : ringItemData) {
                                // ==触发定时任务==
                                JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1,
                                        null, null, null);
                            }

                            ringItemData.clear();
                        }
                    } catch (Exception e) {
                        if (!ringThreadToStop) {
                            log.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e.getMessage(), e);
                        }
                    }
                }

                log.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
            }
        });
        ringThread.setDaemon(true);
        ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }


    /**
     * 刷新定时任务下一次的执行时间
     */
    private void refreshNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        Date nextValidTime = generateNextValidTime(jobInfo, fromTime);
        if (nextValidTime != null) {
            jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
            jobInfo.setTriggerNextTime(nextValidTime.getTime());
        } else {
            jobInfo.setTriggerStatus(0);
            jobInfo.setTriggerLastTime(0);
            jobInfo.setTriggerNextTime(0);
            log.warn(">>>>>>>>>>> xxl-job, refreshNextValidTime fail for job: jobId={}, scheduleType={}, scheduleConf={}", jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf());
        }
    }

    /**
     * 把定时任务放到时间轮中
     */
    private void pushTimeRing(int ringSecond, int jobId) {
        List<Integer> ringItemData = ringData.computeIfAbsent(ringSecond, k -> new ArrayList<>());
        ringItemData.add(jobId);
        log.debug(">>>>>>>>>>> xxl-job, schedule push time-ring : " + ringSecond + " = " + Collections.singletonList(ringItemData));
    }

    /**
     * 停止任务调度器的方法，其实就是终止本类的两个线程
     */
    public void toStop() {
        scheduleThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        if (scheduleThread.getState() != Thread.State.TERMINATED) {
            scheduleThread.interrupt();
            try {
                scheduleThread.join();
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
        boolean hasRingData = false;
        if (!ringData.isEmpty()) {
            for (int second : ringData.keySet()) {
                List<Integer> tmpData = ringData.get(second);
                if (tmpData != null && tmpData.size() > 0) {
                    hasRingData = true;
                    break;
                }
            }
        }
        if (hasRingData) {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
        ringThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        if (ringThread.getState() != Thread.State.TERMINATED) {
            ringThread.interrupt();
            try {
                ringThread.join();
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
        log.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
    }


    // ---------------------- tools ----------------------

    /**
     * 集合cron表达式计算定时任务下一次的执行时间
     */
    public static Date generateNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
        if (ScheduleTypeEnum.CRON == scheduleTypeEnum) {
            return new CronExpression(jobInfo.getScheduleConf()).getNextValidTimeAfter(fromTime);
        } else if (ScheduleTypeEnum.FIX_RATE == scheduleTypeEnum /*|| ScheduleTypeEnum.FIX_DELAY ==scheduleTypeEnum*/) {
            return new Date(fromTime.getTime() + Integer.valueOf(jobInfo.getScheduleConf()) * 1000);
        }
        return null;
    }
}
