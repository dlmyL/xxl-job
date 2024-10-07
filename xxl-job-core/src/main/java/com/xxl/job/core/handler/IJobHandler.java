package com.xxl.job.core.handler;

/**
 * 封装定时任务方法的接口
 */
public abstract class IJobHandler {

	public void init() throws Exception {
		// do something
	}

	public abstract void execute() throws Exception;

	public void destroy() throws Exception {
		// do something
	}
}
