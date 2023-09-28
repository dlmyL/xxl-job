package com.xxl.job.core.handler;

/**
 * <h1>封装定时任务方法的接口</h1>
 */
public abstract class IJobHandler {

	public abstract void execute() throws Exception;

	public void init() throws Exception {
		// do something
	}

	public void destroy() throws Exception {
		// do something
	}

}
