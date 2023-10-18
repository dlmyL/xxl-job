package com.xxl.job.admin.core.route.strategy;

import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.core.route.ExecutorRouter;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;

import java.util.List;

/**
 * <h1>
 * 故障转移策略，xxl-job的故障转移是作为路由策略出现的。
 *
 * 所谓的故障转移，在xxl-job中其实就是在每次调度前，向执行器发送心跳消息，如果消息发送失败，就意味着这个执行器不能使用，
 * 那就紧接着给下一个执行器发送心跳消息，一旦返回成功消息，就直接使用该执行器地址，不会再继续向其他的执行器发送心跳消息了。
 * </h1>
 */
public class ExecutorRouteFailover extends ExecutorRouter {

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        StringBuffer beatResultSB = new StringBuffer();
        for (String address : addressList) {
            // 遍历得到的执行器地址
            ReturnT<String> beatResult = null;
            try {
                // 得到访问执行器的客户端
                ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
                // 向执行器发送心跳检测请求，看执行器是否还在线
                // EXEC => ExecutorBizClient#beat {执行器内嵌服务根地址}/beat
                beatResult = executorBiz.beat();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                beatResult = new ReturnT<>(ReturnT.FAIL_CODE, "" + e);
            }
            beatResultSB.append((beatResultSB.length() > 0) ? "<br><br>" : "")
                    .append(I18nUtil.getString("jobconf_beat") + "：")
                    .append("<br>address：").append(address)
                    .append("<br>code：").append(beatResult.getCode())
                    .append("<br>msg：").append(beatResult.getMsg());

            // 心跳检测没问题，就直接使用该执行器
            if (beatResult.getCode() == ReturnT.SUCCESS_CODE) {
                beatResult.setMsg(beatResultSB.toString());
                beatResult.setContent(address);
                return beatResult;
            }
        }
        return new ReturnT<>(ReturnT.FAIL_CODE, beatResultSB.toString());
    }

}
