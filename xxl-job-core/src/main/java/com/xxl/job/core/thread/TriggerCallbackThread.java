package com.xxl.job.core.thread;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.context.XxlJobContext;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.enums.RegistryConfig;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.util.FileUtil;
import com.xxl.job.core.util.JdkSerializeTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * <h1>回调调度中心的线程</h1>
 *
 * Created by xuxueli on 16/7/22.
 */
public class TriggerCallbackThread {
    private static Logger logger = LoggerFactory.getLogger(TriggerCallbackThread.class);

    private static TriggerCallbackThread instance = new TriggerCallbackThread();
    public static TriggerCallbackThread getInstance(){
        return instance;
    }

    /**
     * 要被回调给调度中心的定时任务执行结果的信息，会封装在 HandlerCallbackParam 对象中，而该对象会先放在该队列中
     */
    private LinkedBlockingQueue<HandleCallbackParam> callBackQueue = new LinkedBlockingQueue<HandleCallbackParam>();

    /**
     * <h2>把封装回调信息的 HandlerCallbackParam 对象提交给 callBackQueue 队列</h2>
     */
    public static void pushCallBack(HandleCallbackParam callback){
        getInstance().callBackQueue.add(callback);
        logger.debug(">>>>>>>>>>> xxl-job, push callback request, logId:{}", callback.getLogId());
    }

    /**
     * 回调线程，就是这个线程把回调信息通过 HTTP 发送给调度中心的
     */
    private Thread triggerCallbackThread;
    /**
     * 重试线程
     */
    private Thread triggerRetryCallbackThread;
    private volatile boolean toStop = false;

    /**
     * <h2>启动该线程组件的方法</h2>
     */
    public void start() {
        // 对访问调度中心的客户端做判空操作
        if (XxlJobExecutor.getAdminBizList() == null) {
            logger.warn(">>>>>>>>>>> xxl-job, executor callback config fail, adminAddresses is null.");
            return;
        }

        // 启动回调线程
        triggerCallbackThread = new Thread(new Runnable() {
            @Override
            public void run() {
                // 正常的回调
                while(!toStop){
                    try {
                        // 从回调任务队列中取出一个回调的信息对象
                        HandleCallbackParam callback = getInstance().callBackQueue.take();
                        if (callback != null) {
                            List<HandleCallbackParam> callbackParamList = new ArrayList<HandleCallbackParam>();
                            /*
                            这里的意思就是说，如果回调的任务队列中有待回调的数据，就把所有数据转移到一个集合中，
                            并且返回有多少条要回调的数据
                            注意：回调 drainTo 方法时，回调队列 callBackQueue 中的数据也被清除了
                             */
                            int drainToNum = getInstance().callBackQueue.drainTo(callbackParamList);
                            /*
                            把最开始取出来的数据再放回去，否则就会落下一个数据了
                            这里要弄清楚，每个定时任务的回调信息都会通过 triggerCallbackThread 这个组件进行回调，
                            所以会有很多回调信息提交给回调队列，当回调的时候，自然也是批量回调
                             */
                            callbackParamList.add(callback);
                            // 进行回调，如果发生错误就会自动重试
                            if (callbackParamList!=null && callbackParamList.size()>0) {
                                // 在这里执行回调给调度中心的操作
                                doCallback(callbackParamList);
                            }
                        }
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }

                // 走到这里，就意味着退出了循环，其实也就意味着 triggerCallbackThread 线程要停止工作了
                try {
                    List<HandleCallbackParam> callbackParamList = new ArrayList<HandleCallbackParam>();
                    // 这里会再次把回调队列中的所有数据都放到新的集合中
                    int drainToNum = getInstance().callBackQueue.drainTo(callbackParamList);
                    if (callbackParamList!=null && callbackParamList.size()>0) {
                        // 最后再回调一次信息给注册中心
                        doCallback(callbackParamList);
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, executor callback thread destroy.");

            }
        });
        triggerCallbackThread.setDaemon(true);
        triggerCallbackThread.setName("xxl-job, executor TriggerCallbackThread");
        triggerCallbackThread.start();


        // 启动重试回调的线程
        triggerRetryCallbackThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(!toStop){
                    try {
                        // 重新回调一次
                        retryFailCallbackFile();
                    } catch (Exception e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }

                    }
                    try {
                        // 信息 30s 再次重试
                        TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                    } catch (InterruptedException e) {
                        if (!toStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, executor retry callback thread destroy.");
            }
        });
        triggerRetryCallbackThread.setDaemon(true);
        triggerRetryCallbackThread.start();

    }

    /**
     * <h2>终止该组件工作的方法</h2>
     */
    public void toStop(){
        toStop = true;
        if (triggerCallbackThread != null) {
            triggerCallbackThread.interrupt();
            try {
                triggerCallbackThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (triggerRetryCallbackThread != null) {
            triggerRetryCallbackThread.interrupt();
            try {
                triggerRetryCallbackThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

    }

    /**
     * <h2>回调定时任务的执行信息给调度中心</h2>
     */
    private void doCallback(List<HandleCallbackParam> callbackParamList){
        boolean callbackRet = false;
        // 获得访问调度中心的客户端集合，并且遍历它们
        for (AdminBiz adminBiz: XxlJobExecutor.getAdminBizList()) {
            try {
                // 在这里进行回调
                ReturnT<String> callbackResult = adminBiz.callback(callbackParamList);
                if (callbackResult!=null && ReturnT.SUCCESS_CODE == callbackResult.getCode()) {
                    // 回调成功了，就记录一下日志
                    callbackLog(callbackParamList, "<br>----------- xxl-job job callback finish.");
                    // 把回调标志位改成 true，说明回调成功了
                    callbackRet = true;
                    break;
                } else {
                    // 回调失败了，记录一下日志
                    callbackLog(callbackParamList, "<br>----------- xxl-job job callback fail, callbackResult:" + callbackResult);
                }
            } catch (Exception e) {
                // 回调异常了，记录一下日志
                callbackLog(callbackParamList, "<br>----------- xxl-job job callback error, errorMsg:" + e.getMessage());
            }
        }
        if (!callbackRet) {
            // 这里就是回调失败了的意思，要把回调失败的数据存储到本地一个专门的文件当中，方便重试线程重新回调
            appendFailCallbackFile(callbackParamList);
        }
    }

    /**
     * <h2>
     *     回调失败的话，记录失败日志
     *     注意：这里记录的失败日志，是把每个日志记录到回调信息对应的每个定时任务的本地日志文件中
     * </h2>
     */
    private void callbackLog(List<HandleCallbackParam> callbackParamList, String logContent){
        for (HandleCallbackParam callbackParam: callbackParamList) {
            // 这里创建日志文件名，可以看到，其实是和定时任务的本地日志文件是一样的，所以就是把信息存储到定时任务日志的本地文件中，一一对应地存储
            String logFileName = XxlJobFileAppender.makeLogFileName(new Date(callbackParam.getLogDateTim()), callbackParam.getLogId());
            // 设置上下文对象，把上下文对象放到线程的私有容器中
            XxlJobContext.setXxlJobContext(new XxlJobContext(
                    -1,
                    null,
                    logFileName,
                    -1,
                    -1));
            // 记录信息到本地日志文件中
            XxlJobHelper.log(logContent);
        }
    }


    // ---------------------- fail-callback file ----------------------

    /**
     * 设置回调失败日志的本地存储路径
     */
    private static String failCallbackFilePath = XxlJobFileAppender.getLogPath().concat(File.separator).concat("callbacklog").concat(File.separator);
    /**
     * 设置回调失败日志存储的文件名
     */
    private static String failCallbackFileName = failCallbackFilePath.concat("xxl-job-callback-{x}").concat(".log");

    /**
     * <h2>回调失败了的意思，就把回调失败的数据存储到本地一个专门的文件当中，方便重试线程重新回调</h2>
     */
    private void appendFailCallbackFile(List<HandleCallbackParam> callbackParamList){
        // 判空校验
        if (callbackParamList==null || callbackParamList.size()==0) {
            return;
        }
        // 把回调数据序列化
        byte[] callbackParamList_bytes = JdkSerializeTool.serialize(callbackParamList);
        // 创建文件名，把 x 用当前时间戳替换
        File callbackLogFile = new File(failCallbackFileName.replace("{x}", String.valueOf(System.currentTimeMillis())));
        if (callbackLogFile.exists()) {
            for (int i = 0; i < 100; i++) {
                callbackLogFile = new File(failCallbackFileName.replace("{x}", String.valueOf(System.currentTimeMillis()).concat("-").concat(String.valueOf(i)) ));
                if (!callbackLogFile.exists()) {
                    break;
                }
            }
        }
        // 把 HandlerCallBackParam 存储到本地
        FileUtil.writeFileContent(callbackLogFile, callbackParamList_bytes);
    }

    /**
     * <h2>重新回调执行结果给调度中心</h2>
     */
    private void retryFailCallbackFile(){
        // 得到文件夹的路径
        File callbackLogPath = new File(failCallbackFilePath);
        if (!callbackLogPath.exists()) {
            return;
        }
        if (callbackLogPath.isFile()) {
            callbackLogPath.delete();
        }
        if (!(callbackLogPath.isDirectory() && callbackLogPath.list()!=null && callbackLogPath.list().length>0)) {
            return;
        }
        // 遍历文件夹中的日志文件
        for (File callbaclLogFile: callbackLogPath.listFiles()) {
            // 读取日志信息
            byte[] callbackParamList_bytes = FileUtil.readFileContent(callbaclLogFile);
            if(callbackParamList_bytes == null || callbackParamList_bytes.length < 1){
                // 如果文件是空的就删除
                callbaclLogFile.delete();
                continue;
            }
            // 反序列化一下
            List<HandleCallbackParam> callbackParamList = (List<HandleCallbackParam>) JdkSerializeTool.deserialize(callbackParamList_bytes, List.class);
            // 删除文件
            callbaclLogFile.delete();
            // 重新回调一次
            doCallback(callbackParamList);
        }

    }

}
