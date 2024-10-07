package com.xxl.job.core.biz.client;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.*;
import com.xxl.job.core.util.XxlJobRemotingUtil;

/**
 * 调度中心 => 执行器
 */
public class ExecutorBizClient implements ExecutorBiz {

    public ExecutorBizClient(String addressUrl, String accessToken) {
        this.addressUrl = addressUrl;
        this.accessToken = accessToken;
        if (!this.addressUrl.endsWith("/")) {
            this.addressUrl = this.addressUrl + "/";
        }
    }

    private String addressUrl;  // 这里的地址就是调度中心的服务地址
    private String accessToken; // TOKEN 令牌，执行器和调度中心两端要一致
    private int timeout = 3;    // 访问超时时间

    @Override
    public ReturnT<String> beat() {
        return XxlJobRemotingUtil.postBody(addressUrl + "beat", accessToken, timeout, "", String.class);
    }

    @Override
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam) {
        return XxlJobRemotingUtil.postBody(addressUrl + "idleBeat", accessToken, timeout, idleBeatParam, String.class);
    }

    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        return XxlJobRemotingUtil.postBody(addressUrl + "run", accessToken, timeout, triggerParam, String.class);
    }

    @Override
    public ReturnT<String> kill(KillParam killParam) {
        return XxlJobRemotingUtil.postBody(addressUrl + "kill", accessToken, timeout, killParam, String.class);
    }

    @Override
    public ReturnT<LogResult> log(LogParam logParam) {
        return XxlJobRemotingUtil.postBody(addressUrl + "log", accessToken, timeout, logParam, LogResult.class);
    }
}
