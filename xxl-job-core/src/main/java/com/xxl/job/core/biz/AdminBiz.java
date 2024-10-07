package com.xxl.job.core.biz;

import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;

import java.util.List;

/**
 * 调度中心 RESTful API，提供给执行器端进行调用
 * API服务位置：com.xxl.job.core.biz.AdminBiz
 *             com.xxl.job.admin.controller.JobApiController
 */
public interface AdminBiz {

    /**
     * 任务回调
     * ------
     * 说明：    执行器执行完任务后，回调任务结果时使用
     * 地址格式：{调度中心根地址}/api/callback
     */
    ReturnT<String> callback(List<HandleCallbackParam> callbackParamList);

    /**
     * 执行器注册
     * ------
     * 说明：    执行器注册时使用，调度中心会实时感知注册成功的执行器并发起任务调度
     * 地址格式：{调度中心根地址}/api/registry
     */
    ReturnT<String> registry(RegistryParam registryParam);

    /**
     * 执行器注册摘除
     * ------
     * 说明：    执行器注册摘除时使用，注册摘除后的执行器不参与任务调度与执行
     * 地址格式：{调度中心根地址}/api/registryRemove
     */
    ReturnT<String> registryRemove(RegistryParam registryParam);

}
