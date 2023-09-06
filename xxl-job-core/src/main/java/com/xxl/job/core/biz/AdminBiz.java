package com.xxl.job.core.biz;

import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;

import java.util.List;

/**
 * <h1>程序内部使用的接口，该接口是调度中心暴露给执行器那一端的</h1>
 *
 * @author xuxueli 2017-07-27 21:52:49
 */
public interface AdminBiz {


    // ---------------------- callback ----------------------

    /**
     * <h2>回调定时任务的执行信息给调度中心的方法</h2>
     */
    public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList);


    // ---------------------- registry ----------------------

    /**
     * <h2>执行器注册自己到调度中心的方法</h2>
     */
    public ReturnT<String> registry(RegistryParam registryParam);

    /**
     * <h2>执行器将自己从调度中心移除的方法</h2>
     */
    public ReturnT<String> registryRemove(RegistryParam registryParam);


    // ---------------------- biz (custome) ----------------------
    // group、job ... manage

}
