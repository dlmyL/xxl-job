package com.xxl.job.core.context;

/**
 * <h1>定时任务的上下文</h1>
 * <p>
 * 每一个定时任务都对应着一个工作线程，而每一个定时任务执行的详细信息和返回结果都是独一份的，既然是这样，
 * 为什么不直接用线程的私有Map来存放呢？也就是ThreadLocal来进行存放。这样，每个线程执行完自己的定时任务后，
 * 就把结果暂时存放到线程的私有Map中，等到定时任务对应的工作线程要处理定时任务的日志信息了，就从本地Map中重新
 * 取出定时任务执行的详细信息。这种处理手段既简单又高效，实在是精妙。
 * </p>
 */
public class XxlJobContext {

    // 200是成功，500是失败，502是超时
    public static final int HANDLE_CODE_SUCCESS = 200;
    public static final int HANDLE_CODE_FAIL = 500;
    public static final int HANDLE_CODE_TIMEOUT = 502;

    // ---------------------- base info ----------------------

    /**
     * job id
     */
    private final long jobId;

    /**
     * job param
     */
    private final String jobParam;

    // ---------------------- for log ----------------------

    /**
     * job log filename
     */
    private final String jobLogFileName;

    // ---------------------- for shard ----------------------

    /**
     * shard index
     */
    private final int shardIndex;

    /**
     * shard total
     */
    private final int shardTotal;

    // ---------------------- for handle ----------------------

    /**
     * handleCode：The result status of job execution
     *
     *      200 : success
     *      500 : fail
     *      502 : timeout
     *
     */
    private int handleCode;

    /**
     * handleMsg：The simple log msg of job execution
     */
    private String handleMsg;


    public XxlJobContext(long jobId, String jobParam, String jobLogFileName, int shardIndex, int shardTotal) {
        this.jobId = jobId;
        this.jobParam = jobParam;
        this.jobLogFileName = jobLogFileName;
        this.shardIndex = shardIndex;
        this.shardTotal = shardTotal;
        /*
        构造方法中唯一值得注意的就是这里，创建 XxlJobContext 对象的时候默认定时任务的执行结果就是成功，
        如果执行失败，会有其他方法把这里设置成失败
         */
        this.handleCode = HANDLE_CODE_SUCCESS;
    }

    public long getJobId() {
        return jobId;
    }

    public String getJobParam() {
        return jobParam;
    }

    public String getJobLogFileName() {
        return jobLogFileName;
    }

    public int getShardIndex() {
        return shardIndex;
    }

    public int getShardTotal() {
        return shardTotal;
    }

    public void setHandleCode(int handleCode) {
        this.handleCode = handleCode;
    }

    public int getHandleCode() {
        return handleCode;
    }

    public void setHandleMsg(String handleMsg) {
        this.handleMsg = handleMsg;
    }

    public String getHandleMsg() {
        return handleMsg;
    }

    // ---------------------- tool ----------------------

    /*
    这里是一个线程的本地变量，因为定时任务真正执行的时候，在执行器端是一个定时任务任务对应一个线程
    这样就把定时任务隔离开了，自然就可以利用这个线程的本地变量，把需要的数据存储在里面
    这里使用的这个变量是可继承的 ThreadLocal，也就子线程可以访问父线程存储在本地的数据了
     */
    private static InheritableThreadLocal<XxlJobContext> contextHolder = new InheritableThreadLocal<>();

    public static void setXxlJobContext(XxlJobContext xxlJobContext){
        contextHolder.set(xxlJobContext);
    }

    public static XxlJobContext getXxlJobContext(){
        return contextHolder.get();
    }

}