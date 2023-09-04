package com.xxl.job.admin.service.impl;

import com.xxl.job.admin.core.thread.JobCompleteHelper;
import com.xxl.job.admin.core.thread.JobRegistryHelper;
import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <h1>这个类是调度中心要使用到的</h1>
 *
 * @author xuxueli 2017-07-27 21:54:20
 */
@Service
public class AdminBizImpl implements AdminBiz {

    /**
     * <h2>把执行器回调的定时任务执行的结果信息收集起来</h2>
     */
    @Override
    public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {
        return JobCompleteHelper.getInstance().callback(callbackParamList);
    }

    /**
     * <h2>把执行器注册到注册中心</h2>
     */
    @Override
    public ReturnT<String> registry(RegistryParam registryParam) {
        // 通过 JobRegistryHelper 组件中创建的线程池来完成注册任务
        return JobRegistryHelper.getInstance().registry(registryParam);
    }

    /**
     * <h2>移除执行器</h2>
     */
    @Override
    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        return JobRegistryHelper.getInstance().registryRemove(registryParam);
    }

}
