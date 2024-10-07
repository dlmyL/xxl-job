package com.xxl.job.core.biz.impl;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.IdleBeatParam;
import com.xxl.job.core.biz.model.KillParam;
import com.xxl.job.core.biz.model.LogParam;
import com.xxl.job.core.biz.model.LogResult;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.glue.GlueTypeEnum;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.impl.GlueJobHandler;
import com.xxl.job.core.handler.impl.ScriptJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.thread.JobThread;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

/**
 * 该类就是在执行器端进行定时任务调用的类
 */
@Slf4j
public class ExecutorBizImpl implements ExecutorBiz {

    @Override
    public ReturnT<String> beat() {
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam) {
        boolean isRunningOrHasQueue = false;

        // 获取执行定时任务的线程
        JobThread jobThread = XxlJobExecutor.loadJobThread(idleBeatParam.getJobId());
        // 如果线程不为null并且正在工作，就把该变量设置为true
        if (jobThread != null && jobThread.isRunningOrHasQueue()) {
            isRunningOrHasQueue = true;
        }

        /*
        这时候就说明调度的任务还没有被执行，肯定在队列里面，或者是正在执行，
        表示当前执行器比较繁忙
         */
        if (isRunningOrHasQueue) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "job thread is running or has trigger queue.");
        }

        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        /*
        获取该定时任务的执行线程 ConcurrentMap<Integer, JobThread> jobThreadRepository
        key=jobId，value=JobThread，也就是说一个定时任务，对应一个JobThread。
         */
        JobThread jobThread = XxlJobExecutor.loadJobThread(triggerParam.getJobId());
        /*
        判断该jobThread是否为空，不为空则说明该定时任务不是第一次
        执行了，也就意味着该线程已经被分配了定时任务。
         */
        IJobHandler jobHandler = jobThread != null ? jobThread.getHandler() : null;

        // 这个变量记录的是移除旧的工作线程的原因
        String removeOldReason = null;

        // 根据定时任务的调度模式执行相应的逻辑
        GlueTypeEnum glueTypeEnum = GlueTypeEnum.match(triggerParam.getGlueType());
        if (GlueTypeEnum.BEAN == glueTypeEnum/*BEAN模式*/) {
            /*
            获取定时任务执行实体 ConcurrentMap<String, IJobHandler> jobHandlerRepository
            key是定时任务的名字，为全局唯一，value是MethodJobHandler对象。
             */
            IJobHandler newJobHandler = XxlJobExecutor.loadJobHandler(triggerParam.getExecutorHandler());
            /*
            这里会进行一下判断，如果上面得到的JobHandler并不为空，说明该定时任务已经执行过了，并且分配
            了对应执行任务的线程，但是根据定时任务的名字从jobHandlerRepository中得到封装定时任务方法
            的对象却和JobHandler不相同，说明定时任务已经改变了。
            定时任务已经改变了， 要做出相应处理，需要把旧的线程杀死，然后把执行定时任务的线程和封装定时
            任务方法的对象都置为null。
             */
            if (jobThread != null && jobHandler != newJobHandler) {
                removeOldReason = "change jobhandler or glue type, and terminate the old job thread.";
                jobThread = null;
                jobHandler = null;
            }

            /*
            ==定时任务第一次执行==
            如果走到这里，就意味着JobHandler为null，这也就意味着上面得到的jobThread为null，这就说明，
            这次调度的定时任务是第一次执行，所以直接让JobHandler等于从jobHandlerRepository这个Map中
            获取newJobHandler即可，然后这个JobHandler会在下面创建JobThread的时候用到。
             */
            if (jobHandler == null) {
                jobHandler = newJobHandler;
                /*
                经过上面的赋值，走到这里如果jobHandler仍然为null，那么只有一个原因，就是执行器这一端根本
                就没有对应的定时任务，通过执行器的名字根本从jobHandlerRepository这个Map中找不到要被执行
                的定时任务。
                 */
                if (jobHandler == null) {
                    return new ReturnT<>(ReturnT.FAIL_CODE, "job handler [" + triggerParam.getExecutorHandler() + "] " + "not found.");
                }
            }
        } else if (GlueTypeEnum.GLUE_GROOVY == glueTypeEnum/*GLUE Java模式*/) {
            /*
            走到这里，说明是glue模式，在线编辑代码然后执行。
            注意：这时候运行的是glue模式，就不能再使用MethodJobHandler反射执行定时任务了，应该使用
            GlueJobHandler来执行任务所以下面会先判断GlueJobHandler中glue的更新时间和本次要执行的
            软任务的更新时间是否相等，如果不相等说明glue的源码可能改动了，需要重新创建handler和对应
            的工作线程。
             */
            if (jobThread != null && !(jobThread.getHandler() instanceof GlueJobHandler
                    && ((GlueJobHandler) jobThread.getHandler()).getGlueUpdatetime() == triggerParam.getGlueUpdatetime())) {
                removeOldReason = "change job source or glue type, and terminate the old job thread.";
                jobThread = null;
                jobHandler = null;
            }

            if (jobHandler == null) {
                try {
                    IJobHandler originJobHandler = GlueFactory.getInstance().loadNewInstance(triggerParam.getGlueSource());
                    jobHandler = new GlueJobHandler(originJobHandler, triggerParam.getGlueUpdatetime());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    return new ReturnT<>(ReturnT.FAIL_CODE, e.getMessage());
                }
            }
        } else if (glueTypeEnum != null && glueTypeEnum.isScript()/*GLUE脚本模式*/) {
            if (jobThread != null && !(jobThread.getHandler() instanceof ScriptJobHandler
                    && ((ScriptJobHandler) jobThread.getHandler()).getGlueUpdatetime() == triggerParam.getGlueUpdatetime())) {
                removeOldReason = "change job source or glue type, and terminate the old job thread.";
                jobThread = null;
                jobHandler = null;
            }

            if (jobHandler == null) {
                jobHandler = new ScriptJobHandler(triggerParam.getJobId(), triggerParam.getGlueUpdatetime(), triggerParam.getGlueSource(), GlueTypeEnum.match(triggerParam.getGlueType()));
            }
        } else /*无调度模式*/{
            // 如果没有合适的调度模式，就返回调用失败的信息
            return new ReturnT<>(ReturnT.FAIL_CODE, "glueType[" + triggerParam.getGlueType() + "] is not valid.");
        }

        // 走到这里只是判断jobThread不为null，说明执行器端已经为该定时任务创建了工作线程
        if (jobThread != null) {
            // 得到定时任务的阻塞策略，默认是单机串行
            ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum.match(triggerParam.getExecutorBlockStrategy(), null);
            if (ExecutorBlockStrategyEnum.DISCARD_LATER == blockStrategy/*丢弃后续调度*/) {
                /*
                走到这里说明定时任务的阻塞队列直接为丢弃，所以接下来要判断一下执行该定时任务的线程
                是否正在工作，如果正在工作并且其内部的队列中有数据，说明该线程执行的定时任务已经被
                调度过几次了，但是还未执行，只能暂时缓存在工作线程的内部队列中。
                 */
                if (jobThread.isRunningOrHasQueue()) {
                    // 因为阻塞策略是直接丢弃的，所以直接返回失败结果
                    return new ReturnT<>(ReturnT.FAIL_CODE, "block strategy effect：" + ExecutorBlockStrategyEnum.DISCARD_LATER.getTitle());
                }
            } else if (ExecutorBlockStrategyEnum.COVER_EARLY == blockStrategy/*覆盖之前调度*/) {
                // 走到这里说明阻塞策略为覆盖，覆盖的意思就是旧的任务不执行了，直接执行这个新的定时任务
                if (jobThread.isRunningOrHasQueue()) {
                    removeOldReason = "block strategy effect：" + ExecutorBlockStrategyEnum.COVER_EARLY.getTitle();
                    /*
                    所以这里直接把工作线程的引用置为null，这样下面就可以创建一个新的工作线程，然后
                    缓存到Map中，新的工作线程就是直接执行新的定时任务了，默认的阻塞策略是串行，都放
                    到工作线程内部的队列中，等待被执行。
                     */
                    jobThread = null;
                }
            }
        }

        if (jobThread == null) {
            /*
            如果走到这里意味着定时任务是第一次执行，还没有创建对应的执行定时任务的线程，所以就在这里把对应
            的线程创建出来，并且缓存到jobThreadRepository这个Map中，在这里就用到了上面赋值过的jobHandler。
             */
            jobThread = XxlJobExecutor.registJobThread(triggerParam.getJobId(), jobHandler, removeOldReason);
        }

        /*
        如果走到这里，不管上面是什么情况，总之jobThread肯定存在了，所以直接
        把要调度的任务放到这个线程内部的队列中，等待线程去调用，并返回结果。
         */
        return jobThread.pushTriggerQueue(triggerParam);
    }

    @Override
    public ReturnT<String> kill(KillParam killParam) {
        // 根据jobId获取到对应的执行任务的线程
        JobThread jobThread = XxlJobExecutor.loadJobThread(killParam.getJobId());
        // 如果线程不为null，从jobThreadRepository缓存中移除线程，并终止该线程
        if (jobThread != null) {
            XxlJobExecutor.removeJobThread(killParam.getJobId(), "scheduling center kill job.");
            return ReturnT.SUCCESS;
        }
        // 返回结果
        return new ReturnT<>(ReturnT.SUCCESS_CODE, "job thread already killed.");
    }

    @Override
    public ReturnT<LogResult> log(LogParam logParam) {
        // 根据定jobId和触发时间创建日志文件名
        String logFileName = XxlJobFileAppender.makeLogFileName(new Date(logParam.getLogDateTim()), logParam.getLogId());
        // 开始从日志文件中读取日志
        LogResult logResult = XxlJobFileAppender.readLog(logFileName, logParam.getFromLineNum());
        // 返回定时任务执行日志
        return new ReturnT<>(logResult);
    }
}
