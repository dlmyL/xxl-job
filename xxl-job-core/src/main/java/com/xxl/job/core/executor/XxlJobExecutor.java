package com.xxl.job.core.executor;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.client.AdminBizClient;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.handler.impl.MethodJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.server.EmbedServer;
import com.xxl.job.core.thread.JobLogFileCleanThread;
import com.xxl.job.core.thread.JobThread;
import com.xxl.job.core.thread.TriggerCallbackThread;
import com.xxl.job.core.util.IpUtil;
import com.xxl.job.core.util.NetUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 执行器启动的入口类，其实是从子类中开始执行，但是子类会调用到父类的#start方法，真正启动执行器组件。
 * <P>
 * 在业务开发中，XXL-JOB都是要集成到SpringBoot中使用的。可以说，XXL-JOB执行器的工作，是从SpringBoot开始的。
 * 当SpringBoot启动的时候，会在SpringBoot容器中的所有单例bean创建之后，启动执行器的服务端，然后执行器就开始
 * 工作了，可以接受调度中心的远程调度，然后执行定时任务。这就是执行器的执行流程。
 * <p>
 * 最终执行器这一端执行定时任务的模式就成了这样：执行器内嵌的Netty服务器接收调度中心发送过来的要执行的定时任务
 * 的信息，然后出发消息的入站事件，会被解码后的消息传递到EmbedHttpServerHandler这个消息处理器中，被这个消息处
 * 理器的channelRead0方法处理。为了避免单线程执行器的工作效率受到影响，在该方法中，会使用我实现定义好的业务线
 * 程池来执行定时任务，但并不是让业务线程池直接执行定时任务。在业务线程池中，会给每一个要执行的定时任务创建对应
 * 的线程，这个线程就用来一直执行这个定时任务。
 * 等后续的调度中心再次调度相同的定时任务时，在执行器这一端仍然是相同的线程来执行这个定时任务。总之，每个定时任
 * 务都会在执行器端对应一个唯一的线程。不管该定时任务被调度几次，总是与其对应的唯一线程来执行它。当然，这样也有
 * 弊端，那就是定时任务本次调用超时了或者阻塞了，后续对该定时任务的调度都会延时。
 * 其实这也称不上是弊端，至少解决了并发问题。再说，还可以设置某些超时策略，来处理这种情况。
 */
@Slf4j
@Setter
public class XxlJobExecutor {

    // 下面这些成员变量都是定义在配置文件中的，而这里的属性会在用户自己定义的XxlJobConfig配置类中被赋值

    private String adminAddresses;// 服务器的地址，也就是调度中心的地址，执行器要注册到调度中心那一端
    private String accessToken;   // TOKEN令牌，这个令牌要和调度中心那一端的令牌配置成一样的
    private String appname;       // 这个是执行器的名称，注册执行器到调度中心的时候，使用的就是这个名称
    private String address;       // 执行器的地址，这个地址在配置文件中为空，表示使用默认地址ip:port
    private String ip;            // 执行器的IP地址
    private int port;             // 执行器的端口号
    private String logPath;       // 执行器的日志收集路径
    private int logRetentionDays; // 执行器日志的保留天数，一般为30天，在配置文件中可以自己设定

    public void start() throws Exception {
        // 初始化日志收集组件，并且把用户设置的存储日志的路径设置到该组件中
        XxlJobFileAppender.initLogPath(logPath);

        /*
        初始化admin连接路径存储集合，如果是在集群环境下，可能会有多个调度中心，
        所以执行器要把自己分别注册到这些调度中心上，这里是根据用户配置的调度中
        心地址，把用来远程注册的客户端初始化好。
         */
        initAdminBizList(adminAddresses, accessToken);

        // 该组件的功能是用来清除执行器端的过期日志的
        JobLogFileCleanThread.getInstance().start(logRetentionDays);

        // 启动回调执行结果信息给调度中心的组件
        TriggerCallbackThread.getInstance().start();

        /*
        启动执行器内部内嵌的Netty服务器，但是构建的是HTTP服务器，仍然是用
        HTTP来传输消息的，在该方法中，会进一步把执行器注册到调度中心上。
         */
        initEmbedServer(address, ip, port, appname, accessToken);
    }

    public void destroy(){
        // 停止内嵌的Netty服务器
        stopEmbedServer();
        // 停止真正执行定时任务的各个线程
        if (!jobThreadRepository.isEmpty()) {
            for (Map.Entry<Integer, JobThread> item: jobThreadRepository.entrySet()) {
                JobThread oldJobThread = removeJobThread(item.getKey(), "web container destroy and kill the job.");
                if (oldJobThread != null) {
                    try {
                        oldJobThread.join();
                    } catch (InterruptedException e) {
                        log.error(">>>>>>>>>>> xxl-job, JobThread destroy(join) error, jobId:{}", item.getKey(), e);
                    }
                }
            }

            jobThreadRepository.clear();
        }

        // 清空缓存jobHandler的Map
        jobHandlerRepository.clear();

        // 停止清除执行器端的过期日志的线程
        JobLogFileCleanThread.getInstance().toStop();
        // 停止回调执行结果信息给调度中心的线程
        TriggerCallbackThread.getInstance().toStop();
    }


    // ========== admin-client (rpc invoker) ==========

    @Getter
    private static List<AdminBiz> adminBizList;

    /**
     * 初始化客户端的方法，初始化的客户端是用来向调度中心发送消息的
     */
    private void initAdminBizList(String adminAddresses, String accessToken) throws Exception {
        if (adminAddresses!=null && adminAddresses.trim().length()>0) {
            // 在这里判断可能有多个调度中心服务器，所以要展开遍历
            for (String address: adminAddresses.trim().split(",")) {
                if (address!=null && address.trim().length()>0) {
                    // 根据服务器地址和令牌创建一个客户端
                    AdminBiz adminBiz = new AdminBizClient(address.trim(), accessToken);
                    // 如果AdminBizList对象为空，就初始化集合对象
                    if (adminBizList == null) {
                        adminBizList = new ArrayList<>();
                    }

                    // 把创建好的客户端添加到集合中
                    adminBizList.add(adminBiz);
                }
            }
        }
    }


    // ========== executor-server (rpc provider) ==========

    // 内嵌的Netty服务器对象
    private EmbedServer embedServer = null;

    /**
     * 初始化并启动执行器端内嵌的Netty服务器，然后把执行器注册到调度中心
     */
    private void initEmbedServer(String address, String ip, int port, String appname, String accessToken) throws Exception {
        /*
        这里就是使用默认地址的操作，因为执行器一端的配置文件中并没有配置执行器
        的地址，所以这里使用工具类得到默认端口号9999。
         */
        port = port>0?port: NetUtil.findAvailablePort(9999);
        // 在这里得到默认IP地址
        ip = (ip!=null&&ip.trim().length()>0)?ip: IpUtil.getIp();

        // 判断地址是否为空
        if (address==null || address.trim().length()==0) {
            // 如果为空说明真的没有配置，那就把刚才得到的IP地址和端口号拼接起来，得到默认的执行器地址
            String ip_port_address = IpUtil.getIpPort(ip, port);
            address = "http://{ip_port}/".replace("{ip_port}", ip_port_address);
        }

        // 校验TOKEN
        if (accessToken==null || accessToken.trim().length()==0) {
            log.warn(">>>>>>>>>>> xxl-job accessToken is empty. To ensure system security, please set the accessToken.");
        }

        // 创建执行器端的Netty服务器
        embedServer = new EmbedServer();
        // ==启动Netty服务器，在启动的过程中，会把执行器注册到调度中心==
        embedServer.start(address, port, appname, accessToken);
    }

    /** 停止内嵌的Netty服务器 */
    private void stopEmbedServer() {
        if (embedServer != null) {
            try {
                embedServer.stop();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }


    // ========== job handler repository ==========

    /**
     * 存放IJobHandler对象的本地缓存。
     * key是定时任务的名字，value是MethodJobHandler对象。
     */
    private static ConcurrentMap<String, IJobHandler> jobHandlerRepository = new ConcurrentHashMap<>();

    public static IJobHandler loadJobHandler(String name){
        return jobHandlerRepository.get(name);
    }

    public static IJobHandler registJobHandler(String name, IJobHandler jobHandler){
        log.info(">>>>>>>>>>> xxl-job register jobhandler success, name:{}, jobHandler:{}", name, jobHandler);
        return jobHandlerRepository.put(name, jobHandler);
    }

    /**
     * 将用户定义的Bean中的每一个定时任务方法都注册到JobHandler的子类对象中。
     */
    protected void registJobHandler(XxlJob xxlJob, Object bean, Method executeMethod){
        // 先判断注解是否为空，为空就直接返回
        if (xxlJob == null) {
            return;
        }

        // 不为空就获取注解的名称，这个名称就是用户定义的当前定时任务的名称
        String name = xxlJob.value();
        // 得到Bean的Class对象
        Class<?> clazz = bean.getClass();
        // 获得定时任务方法的名称，其实定时任务的名称和注解名称也可以定义为相同的，这个没有限制
        String methodName = executeMethod.getName();
        // 对定时任务的名称做判空处理
        if (name.trim().length() == 0) {
            throw new RuntimeException("xxl-job method-jobhandler name invalid, for[" + clazz + "#" + methodName + "] .");
        }

        /*
        从本地缓存了JobHandler的Map中根据定时任务的名字获取JobHandler，如果不为空，
        说明已经存在相同名字的定时任务了，也就是有了对应的JobHandler，所以直接抛出异常。
         */
        if (loadJobHandler(name) != null) {
            throw new RuntimeException("xxl-job jobhandler[" + name + "] naming conflicts.");
        }

        // 设置方法可以被访问，因为后续会根据反射调用
        executeMethod.setAccessible(true);

        /*
        下面声明了初始化方法和销毁方法，那为什么会有这个声明呢？
        因为用户毕竟定义的是IOC容器中的对象，而容器中的对象是可以由用户定义并实现初始化和销毁方法的，
        如果定时任务的注解中也写了初始化和销毁方法，就意味着这个定时任务在执行之前要先执行BEAN对象的
        初始化方法，在结束后要执行BEAN对象的销毁方法，所以这两个方法也要一起注册到JobHandler对象中。
         */
        Method initMethod = null;
        Method destroyMethod = null;
        // 先判断@XxlJob注解中是否写了初始化名称
        if (xxlJob.init().trim().length() > 0) {
            try {
                // 如果有则使用反射从Bean对象中获取相应的初始化方法
                initMethod = clazz.getDeclaredMethod(xxlJob.init());
                // 设置可以访问，因为后续会根据反射调用
                initMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("xxl-job method-jobhandler initMethod invalid, for[" + clazz + "#" + methodName + "] .");
            }
        }
        // 判断有没有定义销毁的方法
        if (xxlJob.destroy().trim().length() > 0) {
            try {
                // 如果有则使用反射从Bean对象中获取相应的初始化方法
                destroyMethod = clazz.getDeclaredMethod(xxlJob.destroy());
                // 设置可以访问，因为后续会根据反射调用
                destroyMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("xxl-job method-jobhandler destroyMethod invalid, for[" + clazz + "#" + methodName + "] .");
            }
        }

        /*
        把得到的定时任务的方法对象、初始化方法对象和销毁方法对象，以及定时任务的名字包装一下，定时任务的
        方法对象，初始化方法对象，和销毁方法对象可以注册到MethodJobHandler中，以后调用时就由这个类的对
        象调用，其实内部还是使用了反射。然后把定时任务的名字和MethodJobHandler对象以键值对的方式缓存在
        jobHandlerRepository这个本地Map中。
         */
        registJobHandler(name, new MethodJobHandler(bean, executeMethod, initMethod, destroyMethod));
    }


    // ========== job thread repository ==========

    /**
     * 缓存JobThread的Map，而每一个定时任务对应着一个ID，也就对应着一个执行这个定时任务的线程。
     * 在这个Map中，key就是定时任务的ID，value就是执行它的线程。
     */
    private static ConcurrentMap<Integer, JobThread> jobThreadRepository = new ConcurrentHashMap<>();

    /**
     * 把定时任务对应的JobThread缓存到jobThreadRepository这个Map中
     */
    public static JobThread registJobThread(int jobId, IJobHandler handler, String removeOldReason){
        // 根据jobId和封装定时任务方法的IJobHandler对象创建JobThread对象
        JobThread newJobThread = new JobThread(jobId, handler);
        // 创建之后就启动线程
        newJobThread.start();

        log.info(">>>>>>>>>>> xxl-job regist JobThread success, jobId:{}, handler:{}", new Object[]{jobId, handler});

        // 将该线程缓存到Map中
        JobThread oldJobThread = jobThreadRepository.put(jobId, newJobThread);
        // 如果oldJobThread不为null，说明Mqp中已经缓存了相同的对象，这里的做法就是直接停止旧线程
        if (oldJobThread != null) {
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();
        }

        return newJobThread;
    }

    /**
     * 从jobThreadRepository这个Map中移除JobThread线程
     */
    public static JobThread removeJobThread(int jobId, String removeOldReason){
        // 根据jobId删除工作线程
        JobThread oldJobThread = jobThreadRepository.remove(jobId);
        // 停止线程，在该方法内部，会将线程的停止条件设置为true，线程就会停下来了
        if (oldJobThread != null) {
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();
            return oldJobThread;
        }

        return null;
    }

    /**
     * 根据定时任务ID，获取对应的JobThread
     */
    public static JobThread loadJobThread(int jobId){
        return jobThreadRepository.get(jobId);
    }
}
