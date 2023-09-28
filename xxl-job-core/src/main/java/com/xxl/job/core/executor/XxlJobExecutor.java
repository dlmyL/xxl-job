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
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * <h1>执行器启动的入口类，其实是从子类中开始执行，但是子类会调用到父类的 start 方法，真正启动执行器组件</h1>
 */
@Slf4j
@Setter
public class XxlJobExecutor  {

    // 下面这些成员变量都是定义在配置文件中的，而这里的属性会在用户自己定义的 XxlJobConfig 配置类中被赋值

    /**
     * 服务器的地址，也就是调度中心的地址，执行器要注册到调度中心那一端
     */
    private String adminAddresses;
    /**
     * TOKEN 令牌，这个令牌要和调度中心那一端的令牌配置成一样的，否则调度中心那端校验不通过会报错
     */
    private String accessToken;
    /**
     * 这个是执行器的名称，注册执行器到调度中心的时候，使用的就是这个名称
     */
    private String appname;
    /**
     * 执行器的地址，这个地址在配置文件中为空，表示使用默认地址：ip:port
     */
    private String address;
    /**
     * 执行器的 IP 地址
     */
    private String ip;
    /**
     * 执行器的端口号
     */
    private int port;
    /**
     * 执行器的日志收集地址
     */
    private String logPath;
    /**
     * 执行器日志的保留天数，一般为 30 天，在配置文件中可以自己设定
     */
    private int logRetentionDays;

    // ---------------------- start + stop ----------------------

    /**
     * <h2>启动执行器</h2>
     */
    public void start() throws Exception {
        // 初始化日志收集组件，并且把用户设置的存储日志的路径设置到该组件中
        XxlJobFileAppender.initLogPath(logPath);

        // 初始化 admin 连接路径存储集合，如果是在集群环境下，可能会有多个调度中心，所以执行器要把自己分别注册到这些调度中心上
        // 这里是根据用户配置的调度中心地址，把用来远程注册的客户端初始化好
        initAdminBizList(adminAddresses, accessToken);

        // 该组件的功能是用来清除执行器端的过期日志的
        JobLogFileCleanThread.getInstance().start(logRetentionDays);

        // 启动回调执行结果信息给调度中心的组件
        TriggerCallbackThread.getInstance().start();

        // 启动执行器内部内嵌的 Netty 服务器，但是构建的是 HTTP 服务器，仍然是用 HTTP 来传输消息的
        // 在该方法中，会进一步把执行器注册到调度中心上
        initEmbedServer(address, ip, port, appname, accessToken);
    }

    /**
     * <h2>销毁执行器</h2>
     */
    public void destroy(){
        // 停止内嵌的 Netty 服务器
        stopEmbedServer();

        // 停止真正执行定时任务的各个线程
        if (jobThreadRepository.size() > 0) {
            for (Map.Entry<Integer, JobThread> item: jobThreadRepository.entrySet()) {
                JobThread oldJobThread = removeJobThread(item.getKey(), "web container destroy and kill the job.");
                // wait for job thread push result to callback queue
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
        // 清空缓存 jobHandler 的 Map
        jobHandlerRepository.clear();

        // 停止清除执行器端的过期日志的线程
        JobLogFileCleanThread.getInstance().toStop();

        // 停止回调执行结果信息给调度中心的线程
        TriggerCallbackThread.getInstance().toStop();
    }


    // ---------------------- admin-client (rpc invoker) ----------------------
    /**
     * 该成员变量是用来存放 AdminBizClient 对象的，而该对象是用来向调度中心发送注册信息的
     */
    private static List<AdminBiz> adminBizList;

    /**
     * <h2>初始化客户端的方法，初始化的客户端是用来向调度中心发送消息的</h2>
     */
    private void initAdminBizList(String adminAddresses, String accessToken) throws Exception {
        if (adminAddresses!=null && adminAddresses.trim().length()>0) {
            // 在这里判断可能有多个调度中心服务器，所以要展开遍历
            for (String address: adminAddresses.trim().split(",")) {
                if (address!=null && address.trim().length()>0) {
                    // 根据服务器地址和令牌创建一个客户端
                    AdminBiz adminBiz = new AdminBizClient(address.trim(), accessToken);
                    // 如果 AdminBizList 对象为空，就初始化集合对象
                    if (adminBizList == null) {
                        adminBizList = new ArrayList<AdminBiz>();
                    }
                    // 把创建好的客户端添加到集合中
                    adminBizList.add(adminBiz);
                }
            }
        }
    }

    public static List<AdminBiz> getAdminBizList(){
        return adminBizList;
    }

    // ---------------------- executor-server (rpc provider) ----------------------
    /**
     * 内嵌的服务器对象
     */
    private EmbedServer embedServer = null;

    /**
     * <h2>启动执行器内嵌的 Netty 服务器，然后把执行器注册到调度中心</h2>
     */
    private void initEmbedServer(String address, String ip, int port, String appname, String accessToken) throws Exception {
        // 这里就是使用默认地址的操作，因为执行器一端的配置文件中并没有配置执行器的地址
        // 所以这里使用工具类得到默认端口号 9999
        port = port>0?port: NetUtil.findAvailablePort(9999);
        // 在这里得到默认 IP 地址
        ip = (ip!=null&&ip.trim().length()>0)?ip: IpUtil.getIp();

        // 判断地址是否为空
        if (address==null || address.trim().length()==0) {
            // 如果为空说明真的没有配置，那就把刚才得到的 IP 地址和端口号拼接起来，得到默认的执行器地址
            String ip_port_address = IpUtil.getIpPort(ip, port);
            address = "http://{ip_port}/".replace("{ip_port}", ip_port_address);
        }

        // 校验 TOKEN
        if (accessToken==null || accessToken.trim().length()==0) {
            log.warn(">>>>>>>>>>> xxl-job accessToken is empty. To ensure system security, please set the accessToken.");
        }

        // 创建执行器端的 Netty 服务器
        embedServer = new EmbedServer();
        // 启动服务器，在启动的过程中，会把执行器注册到调度中心
        embedServer.start(address, port, appname, accessToken);
    }

    /**
     * <h2>停止 Netty 服务器运行的方法</h2>
     */
    private void stopEmbedServer() {
        // stop provider factory
        if (embedServer != null) {
            try {
                embedServer.stop();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }


    // ---------------------- job handler repository ----------------------
    /**
     * 存放 IJobHandler 对象的 Map
     */
    private static ConcurrentMap<String, IJobHandler> jobHandlerRepository = new ConcurrentHashMap<String, IJobHandler>();
    public static IJobHandler loadJobHandler(String name){
        return jobHandlerRepository.get(name);
    }
    public static IJobHandler registJobHandler(String name, IJobHandler jobHandler){
        log.info(">>>>>>>>>>> xxl-job register jobhandler success, name:{}, jobHandler:{}", name, jobHandler);
        return jobHandlerRepository.put(name, jobHandler);
    }

    /**
     * <h2>该方法就是用来将用户定义的 BEAN 中的每一个定时任务方法都注册到 JobHandler 的子类对象中</h2>
     */
    protected void registJobHandler(XxlJob xxlJob, Object bean, Method executeMethod){
        // 先判断注解是否为空，为空就直接返回
        if (xxlJob == null) {
            return;
        }
        // 不为空就获取注解的名称，这个名称就是用户定义的当前定时任务的名称
        String name = xxlJob.value();
        // 得到 BEAN 的 Class 对象
        Class<?> clazz = bean.getClass();
        // 获得定时任务方法的名称，其实定时任务的名称和注解名称也可以定义为相同的，这个没有限制
        String methodName = executeMethod.getName();
        // 对定时任务的名称做判空处理
        if (name.trim().length() == 0) {
            throw new RuntimeException("xxl-job method-jobhandler name invalid, for[" + clazz + "#" + methodName + "] .");
        }
        // 从缓存 JobHandler 的 Map 中根据定时任务的名字获取 JobHandler
        if (loadJobHandler(name) != null) {
            // 如果不为空，说明已经存在相同名字的定时任务了，也就是有了对应的 JobHandler，所以直接抛出异常
            throw new RuntimeException("xxl-job jobhandler[" + name + "] naming conflicts.");
        }
        // 设置方法可以被访问
        executeMethod.setAccessible(true);

        /*
            下面声明了初始化方法和销毁方法，那为什么会有这个声明呢？
                因为用户毕竟定义的是 IOC 容器中的对象，而容器中的对象是可以由用户定义并实现初始化和销毁方法的，
                如果定时任务的注解中也写了初始化和销毁方法，就意味着这个定时任务在执行之前要先执行 BEAN 对象的初始化方法，
                在结束后要执行 BEAN 对象的销毁方法，所以这两个方法也要一起注册到 JobHandler 对象中
         */
        Method initMethod = null;
        Method destroyMethod = null;
        // 先判断 @XxlJob 注解中是否写了初始化名称
        if (xxlJob.init().trim().length() > 0) {
            try {
                // 如果有则使用反射从 BEAN 对象中获取相应的初始化方法
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
                // 如果有就使用反射获取
                destroyMethod = clazz.getDeclaredMethod(xxlJob.destroy());
                // 设置可访问
                destroyMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("xxl-job method-jobhandler destroyMethod invalid, for[" + clazz + "#" + methodName + "] .");
            }
        }

        /*
            把得到的定时任务的方法对象、初始化方法对象和销毁方法对象，以及定时任务的名字包装一下，
            定时任务的方法对象，初始化方法对象，和销毁方法对象可以注册到 MethodJobHandler 中，以后调用时就由这个类的对象调用，其实内部还是使用了反射。
            然后把定时任务的名字和 MethodJobHandler 对象以键值对的方式缓存在 jobHandlerRepository 这个 Map 中。
         */
        registJobHandler(name, new MethodJobHandler(bean, executeMethod, initMethod, destroyMethod));

    }


    // ---------------------- job thread repository ----------------------
    /**
     * 缓存 JobThread 的 Map，而每一个定时任务对应着一个 ID，也就对应着一个执行这个定时任务的线程。
     * 在这个 Map 中，key 就是定时任务的 ID，value 就是执行它的线程
     */
    private static ConcurrentMap<Integer, JobThread> jobThreadRepository = new ConcurrentHashMap<Integer, JobThread>();

    /**
     * <h2>把定时任务对应的 JobThread 缓存到 jobThreadRepository 这个 Map 中</h2>
     */
    public static JobThread registJobThread(int jobId, IJobHandler handler, String removeOldReason){
        // 根据定时任务 ID 和封装定时任务方法的 IJobHandler 对象创建 JobThread 对象
        JobThread newJobThread = new JobThread(jobId, handler);
        // 创建之后就启动线程
        newJobThread.start();
        log.info(">>>>>>>>>>> xxl-job regist JobThread success, jobId:{}, handler:{}", new Object[]{jobId, handler});

        // 将该线程缓存到 Map 中
        JobThread oldJobThread = jobThreadRepository.put(jobId, newJobThread);	// putIfAbsent | oh my god, map's put method return the old value!!!
        if (oldJobThread != null) {
            // 如果 oldJobThread 不为 null，说明 Mqp 中已经缓存了相同的对象，这里的做法就是直接停止旧线程
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();
        }

        return newJobThread;
    }

    /**
     * <h2>从 jobThreadRepository 这个 Map 中移除 JobThread 线程</h2>
     */
    public static JobThread removeJobThread(int jobId, String removeOldReason){
        // 根据定时任务 ID 删除工作线程
        JobThread oldJobThread = jobThreadRepository.remove(jobId);
        if (oldJobThread != null) {
            // 停止线程，在该方法内部，会将线程的停止条件设置为 true，线程就会停下来了
            oldJobThread.toStop(removeOldReason);
            oldJobThread.interrupt();
            return oldJobThread;
        }
        return null;
    }

    /**
     * <h2>根据定时任务 ID，获取对应的 JobThread 对象</h2>
     */
    public static JobThread loadJobThread(int jobId){
        return jobThreadRepository.get(jobId);
    }

}
