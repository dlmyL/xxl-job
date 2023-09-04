package com.xxl.job.admin.controller;

import com.xxl.job.admin.controller.annotation.PermissionLimit;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.util.GsonTool;
import com.xxl.job.core.util.XxlJobRemotingUtil;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * <h1>这个类不对 Web 界面进行开放，而是程序内部执行远程调用时使用的，这个类中的接口只对执行器那一端暴露</h1>
 *
 * Created by xuxueli on 17/5/10.
 */
@Controller
@RequestMapping("/api")
public class JobApiController {

    @Resource
    private AdminBiz adminBiz;

    /**
     * <h2>该方法就是执行注册执行器的方法，执行器那一端会访问该接口进行回调、注册、注销</h2>
     */
    @RequestMapping("/{uri}")
    @ResponseBody
    @PermissionLimit(limit=false)
    public ReturnT<String> api(HttpServletRequest request, @PathVariable("uri") String uri, @RequestBody(required = false) String data) {
        // 判断是否为 POST 请求
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
        }
        // 对路径判空处理
        if (uri==null || uri.trim().length()==0) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
        }
        // 判断执行器配置的 TOKEN 和调度中心的是否相等
        if (XxlJobAdminConfig.getAdminConfig().getAccessToken()!=null
                && XxlJobAdminConfig.getAdminConfig().getAccessToken().trim().length()>0
                && !XxlJobAdminConfig.getAdminConfig().getAccessToken().equals(request.getHeader(XxlJobRemotingUtil.XXL_JOB_ACCESS_TOKEN))) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "The access token is wrong.");
        }

        // 判断是否为执行器端回调并上报执行结果
        if ("callback".equals(uri)) {
            List<HandleCallbackParam> callbackParamList = GsonTool.fromJson(data, List.class, HandleCallbackParam.class);
            return adminBiz.callback(callbackParamList);
        }
        // 判断是否为注册操作
        else if ("registry".equals(uri)) {
            RegistryParam registryParam = GsonTool.fromJson(data, RegistryParam.class);
            // 执行注册任务
            return adminBiz.registry(registryParam);
        }
        // 判断是否为移除执行器操作
        else if ("registryRemove".equals(uri)) {
            RegistryParam registryParam = GsonTool.fromJson(data, RegistryParam.class);
            // 执行移除任务
            return adminBiz.registryRemove(registryParam);
        } else {
            // 都不匹配则返回失败
            return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping("+ uri +") not found.");
        }

    }

}
