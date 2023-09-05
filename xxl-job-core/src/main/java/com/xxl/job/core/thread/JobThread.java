package com.xxl.job.core.thread;

import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.context.XxlJobContext;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

/**
 * <h1>该类就是用来真正执行定时任务的，并且是一个定时任务对应着一个 JobThread 对象</h1>
 * <p>
 *  	其实说一个并不太准确，比如，有一个定时任务每2s执行一次，那么在执行器这一端，定时任务对应的JobThread对象一但创建了
 *  就会只执行这个定时任务，但是有可能这个任务比较耗时，3秒还没执行完，那么之后每2秒要执行的这个定时任务可能就会放在JobThread对象中的
 *  队列中等待执行，由此也就引申出了阻塞策略，是选择覆盖还是直接丢弃等等
 *  该类继承了thread，本身就是一个线程
 * </p>
 *
 * @author xuxueli 2016-1-16 19:52:47
 */
public class JobThread extends Thread{
	private static Logger logger = LoggerFactory.getLogger(JobThread.class);

	/**
	 * 定时任务的 ID
	 */
	private int jobId;
	/**
	 * 封装了定时任务方法的对象，BEAN 对象的初始化方法和销毁方法也在该类中
	 */
	private IJobHandler handler;
	/**
	 * 存放触发器参数的一个队列，有可能一个任务比较耗时，3 秒还没执行完，但调度周期是 2 秒，
	 * 那么之后每 2 秒要执行的这个定时任务可能就会放在 JobThread 对象中的队列中等待执行，
	 * 其实存放的就是触发器参数，触发器参数中有待执行的定时任务的名称
	 */
	private LinkedBlockingQueue<TriggerParam> triggerQueue;
	/**
	 * 定时任务的地址 ID 集合
	 */
	private Set<Long> triggerLogIdSet;
	/**
	 * 线程终止标记
	 */
	private volatile boolean toStop = false;
	/**
	 * 线程停止的原因
	 */
	private String stopReason;
	/**
	 * 线程是否正在工作的标记。
	 * 注意，这个标记并不是只线程是否启动或销毁，而是指线程是否正在执行定时任务
	 */
    private boolean running = false;
	/**
	 * 线程的空闲时间，默认为 0
	 */
	private int idleTimes = 0;

	public JobThread(int jobId, IJobHandler handler) {
		this.jobId = jobId;
		this.handler = handler;
		// 初始化队列
		this.triggerQueue = new LinkedBlockingQueue<TriggerParam>();
		// 初始化集合
		this.triggerLogIdSet = Collections.synchronizedSet(new HashSet<Long>());
		// 设置工作线程名字
		this.setName("xxl-job, JobThread-"+jobId+"-"+System.currentTimeMillis());
	}
	public IJobHandler getHandler() {
		return handler;
	}

	/**
	 * <h2>把触发器参数放进队列中</h2>
	 */
	public ReturnT<String> pushTriggerQueue(TriggerParam triggerParam) {
		// 先判断 set 集合中是否包含定时任务的地址 ID，如果包含就说明定时任务正在执行
		if (triggerLogIdSet.contains(triggerParam.getLogId())) {
			logger.info(">>>>>>>>>>> repeate trigger job, logId:{}", triggerParam.getLogId());
			// 返回失败信息，定时任务重复了
			return new ReturnT<String>(ReturnT.FAIL_CODE, "repeate trigger job, logId:" + triggerParam.getLogId());
		}
		// 不包含则将定时任务的日志 ID 放到集合中
		triggerLogIdSet.add(triggerParam.getLogId());
		// 在这里把定时任务放进队列中
		triggerQueue.add(triggerParam);
        return ReturnT.SUCCESS;
	}

	/**
	 * <h2>终止该线程</h2>
	 */
	public void toStop(String stopReason) {
		/*
		   Thread.interrupt只支持终止线程的阻塞状态(wait、join、sleep)，
		   在阻塞出抛出InterruptedException异常,但是并不会终止运行的线程本身；
		   所以需要注意，此处彻底销毁本线程，需要通过共享变量方式；
		 */
		this.toStop = true;
		this.stopReason = stopReason;
	}

	/**
	 * <h2>判断线程是否有任务，并且是否正在运行，这个方法会和阻塞策略一起使用</h2>
	 */
	public boolean isRunningOrHasQueue() {
        return running || triggerQueue.size()>0;
    }

	/**
	 * <h2>当前线程启动之后会执行的 run 方法</h2>
	 */
	@Override
	public void run() {
    	try {
			// 如果 IJobHandler 对象中封装了 BEAN 对象的初始化方法，并且该定时任务注解中也声明了初始化方法要执行
			// 就在这里反射调用 BEAN 对象的初始化方法
			handler.init();
		} catch (Throwable e) {
    		logger.error(e.getMessage(), e);
		}

		// 现在就要在一个循环中不断的从触发器队列中取出待执行的定时任务，开始执行
		while(!toStop){
			// 线程是否工作的标记，默认为false
			running = false;
			// 这个是线程的空闲时间
			idleTimes++;
			// 先声明一个触发器参数变量
            TriggerParam triggerParam = null;
            try {
				// 从触发器参数队列中取出一个触发器参数对象
				// 这里是限时的阻塞获取，如果超过3秒没获取到，就不阻塞了
				triggerParam = triggerQueue.poll(3L, TimeUnit.SECONDS);
				if (triggerParam!=null) {
					// 走到这里，说明获得了触发器参数，这时候就把线程正在执行的标记设置为 true
					running = true;
					// 空闲时间也可以置为 0 了
					idleTimes = 0;
					// 因为定时任务要执行了，所以要把它的日志 ID 先从 set 集合中删除
					triggerLogIdSet.remove(triggerParam.getLogId());

					// 接下来就是一系列的处理执行器端定时任务执行的日志操作

					// 先根据定时任务的触发时间和定时任务的日志 ID，创建一个记录定时任务日的文件名
					String logFileName = XxlJobFileAppender.makeLogFileName(new Date(triggerParam.getLogDateTime()), triggerParam.getLogId());
					// 然后创建一个定时任务上下文对象
					XxlJobContext xxlJobContext = new XxlJobContext(
							triggerParam.getJobId(),
							triggerParam.getExecutorParams(),
							logFileName,
							triggerParam.getBroadcastIndex(),
							triggerParam.getBroadcastTotal());

					// 先把创建出来的定时任务上下文对象存储到执行定时任务线程的私有容器中
					XxlJobContext.setXxlJobContext(xxlJobContext);

					// //这里会向 logFileName 文件中记录一下日志，记录的就是下面的这句话，定时任务开始执行了
					XxlJobHelper.log("<br>----------- xxl-job job execute start -----------<br>----------- Param:" + xxlJobContext.getJobParam());

					// 如果设置了超时时间，就要设置一个新的线程来执行定时任务
					if (triggerParam.getExecutorTimeout() > 0) {
						Thread futureThread = null;
						try {
							FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
								@Override
								public Boolean call() throws Exception {
									// 子线程可以访问父线程的本地变量
									XxlJobContext.setXxlJobContext(xxlJobContext);
									//在 FutureTask 中执行定时任务
									handler.execute();
									return true;
								}
							});
							// 创建线程并且启动线程
							futureThread = new Thread(futureTask);
							futureThread.start();
							// 最多等待用户设置的超时时间
							Boolean tempResult = futureTask.get(triggerParam.getExecutorTimeout(), TimeUnit.SECONDS);
						} catch (TimeoutException e) {
							XxlJobHelper.log("<br>----------- xxl-job job execute timeout");
							XxlJobHelper.log(e);
							// 超时直接设置任务执行超时
							XxlJobHelper.handleTimeout("job execute timeout ");
						} finally {
							futureThread.interrupt();
						}
					} else {
						// 没有设置超时时间，通过反射执行了定时任务，终于在这里执行了
						handler.execute();
					}

					// 定时任务执行了，所以这里要判断一下执行结果是什么，
					// 注意，这里的 XxlJobContext 上下文对象从创建的时候就默认执行结果为成功。
					// 在源码中，在这行代码之前其实还有任务执行超时时间的判断，开启一个子线程去执行定时任务
					// 然后再判断任务执行成功了没，如果没成功 XxlJobHelper 类就会修改上下文对象的执行结果。
					if (XxlJobContext.getXxlJobContext().getHandleCode() <= 0) {
						XxlJobHelper.handleFail("job handle result lost.");
					} else {
						// 走到这里意味着定时任务执行成功了，从定时任务上下文中取出执行的结果信息
						String tempHandleMsg = XxlJobContext.getXxlJobContext().getHandleMsg();
						// 这里有一个三元运算，会判断执行结果信息是不是 null，如果执行成功，毫无异常，这个结果信息就会是 null
						// 只有在执行失败的时候，才会有失败信息被 XxlJobHelper 记录进去
						tempHandleMsg = (tempHandleMsg!=null&&tempHandleMsg.length()>50000)
								?tempHandleMsg.substring(0, 50000).concat("...")
								:tempHandleMsg;
						// 这里是执行成功了，所以得到的是 null，赋值其实就是什么也没赋成
						XxlJobContext.getXxlJobContext().setHandleMsg(tempHandleMsg);
					}
					// 走到这里，不管是执行成功还是失败，都要把结果存储到对应的日志文件中
					// 走到这里大家也应该意识到了，执行器这一端执行的定时任务，实际上是每一个定时任务都会对应一个本地的日志文件，
					// 每个定时任务的执行结果都会存储在自己的文件中，当然，一个定时任务可能会执行很多次，所以定时任务对应的日志文件
					// 就会记录这个定时任务每次执行的信息
					XxlJobHelper.log("<br>----------- xxl-job job execute end(finish) -----------<br>----------- Result: handleCode="
							+ XxlJobContext.getXxlJobContext().getHandleCode()
							+ ", handleMsg = "
							+ XxlJobContext.getXxlJobContext().getHandleMsg()
					);
				} else {
					// 走到这里说明触发器队列中没有数据，也就意味着没有要执行的定时任务
					// 如果线程的空闲时间大于 30 次，这里指的是循环的次数，每循环一次空闲时间就自增 1，
					// 有定时任务被执行空闲时间就清零，不可能没任务线程空转，太浪费资源了
					if (idleTimes > 30) {
						// 而且触发器队列也没有数据
						if(triggerQueue.size() == 0) {
							// 就从缓存 JobThread 线程的 jobThreadRepository这 个 Map 中移除缓存的 JobThread 线程
							// 在移除的时候，会调用该线程的 toStop 方法和 interrupt 方法，让线程真正停下来
							XxlJobExecutor.removeJobThread(jobId, "excutor idel times over limit.");
						}
					}
				}
			} catch (Throwable e) {
				// 如果线程停止了，就记录线程停止的日志到定时任务对应的日志文件中
				if (toStop) {
					XxlJobHelper.log("<br>----------- JobThread toStop, stopReason:" + stopReason);
				}

				// 下面就是将异常信息记录到日志文件中的操作，因为这些都是在 catch 中执行的
				// 就意味着肯定有异常了，所以要记录异常信息
				StringWriter stringWriter = new StringWriter();
				e.printStackTrace(new PrintWriter(stringWriter));
				String errorMsg = stringWriter.toString();

				XxlJobHelper.handleFail(errorMsg);
				// 在这里记录异常信息到日志文件中
				XxlJobHelper.log("<br>----------- JobThread Exception:" + errorMsg + "<br>----------- xxl-job job execute end(error) -----------");
			} finally {
				// 这里就走到了 finally 中，也就要开始执行日志回调给调度中心的操作了
				// 别忘了，调度中心在远程调用之前创建了 XxlJobLog 这个对象，这个对象要记录很多日记调用信息的
                if(triggerParam != null) {
						// 这里要再次判断线程是否停止运行
                    if (!toStop) {
						// 如果没有停止，就创建封装回调信息的HandleCallbackParam对象
						// 把这个对象提交给 TriggerCallbackThread 内部的 callBackQueue 队列中
                        TriggerCallbackThread.pushCallBack(new HandleCallbackParam(
                        		triggerParam.getLogId(),
								triggerParam.getLogDateTime(),
								XxlJobContext.getXxlJobContext().getHandleCode(),
								XxlJobContext.getXxlJobContext().getHandleMsg() )
						);
                    } else {
						// 如果走到这里说明线程被终止了，就要封装处理失败的回信
						TriggerCallbackThread.pushCallBack(new HandleCallbackParam(
                        		triggerParam.getLogId(),
								triggerParam.getLogDateTime(),
								XxlJobContext.HANDLE_CODE_FAIL,
								stopReason + " [job running, killed]" )
						);
                    }
                }
            }
        }

		// 代码走到这里就意味着退出了线程工作的 while 循环，虽然线程还未完全执行完 run 方法，但是已经意味着线程要停止了
		// 判断触发器参数的队列是否为空
		while(triggerQueue !=null && triggerQueue.size()>0){
			// 不为空就取出一个触发器参数
			TriggerParam triggerParam = triggerQueue.poll();
			if (triggerParam!=null) {
				// 下面就是封装回调信息，把执行结果回调给调度中心
				// 这里的意思很简单，因为线程已经终止了，但是调用的定时任务还有没执行完的，要告诉调度中心
				TriggerCallbackThread.pushCallBack(new HandleCallbackParam(
						triggerParam.getLogId(),
						triggerParam.getLogDateTime(),
						XxlJobContext.HANDLE_CODE_FAIL,
						stopReason + " [job not executed, in the job queue, killed.]")
				);
			}
		}

		try {
			// 执行 BEAN 对象的销毁方法
			handler.destroy();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}

		logger.info(">>>>>>>>>>> xxl-job JobThread stoped, hashCode:{}", Thread.currentThread());
	}

}
