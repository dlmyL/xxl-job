package com.xxl.job.admin.controller;

import com.xxl.job.admin.controller.annotation.PermissionLimit;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.util.GsonTool;
import com.xxl.job.core.util.XxlJobRemotingUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * 这个类不对Web界面进行开放，而是程序内部执行远程调用时使用的，只对执行器那一端暴露。
 */
@Controller
@RequiredArgsConstructor
@RequestMapping("/api")
public class JobApiController {

    private final AdminBiz adminBiz;

    /**
     * 该方法就是执行注册执行器的方法，执行器那一端会访问该接口进行回调、注册、注销
     */
    @RequestMapping("/{uri}")
    @ResponseBody
    @PermissionLimit(limit = false)
    public ReturnT<String> api(HttpServletRequest request,
                               @PathVariable("uri") String uri,
                               @RequestBody(required = false) String data) {
        // 判断是否为POST请求
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
        }

        // 对请求路径判空处理
        if (uri == null || uri.trim().length() == 0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
        }

        // 判断执行器配置的token和调度中心的是否相等
        if (XxlJobAdminConfig.getAdminConfig().getAccessToken() != null
                && XxlJobAdminConfig.getAdminConfig().getAccessToken().trim().length() > 0
                && !XxlJobAdminConfig.getAdminConfig().getAccessToken()
                .equals(request.getHeader(XxlJobRemotingUtil.XXL_JOB_ACCESS_TOKEN))) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "The access token is wrong.");
        }

        switch (uri) {
            // ==执行器执行结果回调==
            case "callback":
                List<HandleCallbackParam> callbackParamList = GsonTool.fromJson(data, List.class, HandleCallbackParam.class);
                return adminBiz.callback(callbackParamList);
            // ==执行器注册==
            case "registry": {
                RegistryParam registryParam = GsonTool.fromJson(data, RegistryParam.class);
                return adminBiz.registry(registryParam);
            }
            // ==执行器注销==
            case "registryRemove": {
                RegistryParam registryParam = GsonTool.fromJson(data, RegistryParam.class);
                return adminBiz.registryRemove(registryParam);
            }
            // 请求路径都不匹配则返回失败
            default:
                return new ReturnT<>(ReturnT.FAIL_CODE, "invalid request, uri-mapping(" + uri + ") not found.");
        }
    }
}
