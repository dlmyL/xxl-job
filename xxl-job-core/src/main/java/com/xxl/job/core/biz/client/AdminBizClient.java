package com.xxl.job.core.biz.client;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.util.XxlJobRemotingUtil;

import java.util.List;

/**
 * 执行器 => 调度中心
 */
public class AdminBizClient implements AdminBiz {

    public AdminBizClient(String addressUrl, String accessToken) {
        this.addressUrl = addressUrl;
        this.accessToken = accessToken;
        if (!this.addressUrl.endsWith("/")) {
            this.addressUrl = this.addressUrl + "/";
        }
    }

    private String addressUrl;  // 这里的地址就是调度中心的服务地址
    private String accessToken; // TOKEN 令牌，执行器和调度中心两端要一致
    private int timeout = 3;    // 访问超时时间

    /**
     * 回调定时任务的执行信息给调度中心
     */
    @Override
    public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {
        return XxlJobRemotingUtil.postBody(addressUrl + "api/callback",
                accessToken, timeout, callbackParamList, String.class);
    }

    /**
     * 将执行器注册到调度中心的方法
     */
    @Override
    public ReturnT<String> registry(RegistryParam registryParam) {
        return XxlJobRemotingUtil.postBody(addressUrl + "api/registry",
                accessToken, timeout, registryParam, String.class);
    }

    /**
     * 通知调度中心，把该执行器移除
     */
    @Override
    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        return XxlJobRemotingUtil.postBody(addressUrl + "api/registryRemove",
                accessToken, timeout, registryParam, String.class);
    }
}
