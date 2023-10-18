package com.xxl.job.admin.core.trigger;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.route.ExecutorRouteStrategyEnum;
import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.util.IpUtil;
import com.xxl.job.core.util.ThrowableUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

/**
 * 这个类也是 xxl-job 中很重要的一个类，job 的远程调用就是在该类中进行的，当然不是直接进行，远程调用
 * 到最后，任务还是在执行器那边执行的，但是该类会为远程调用做很多必要的辅助性工作，比如选择路由策略，
 * 然后选择要执行的执行器地址
 */
@Slf4j
public class XxlJobTrigger {

    /**
     * 该方法是远程调用前的准备阶段，在该方法内，如果用户自己设置了执行器的地址和执行器的任务参数，
     * 以及分片策略，在该方法内会对这些操作进行处理
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount,
                               String executorShardingParam, String executorParam, String addressList) {
        // 根据任务ID从数据库中查询到该任务的完整信息
        XxlJobInfo jobInfo = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(jobId);
        // 如果任务为null，则打印一条警告信息后直接退出
        if (jobInfo == null) {
            log.warn(">>>>>>>>>>>> trigger fail, jobId invalid，jobId={}", jobId);
            return;
        }

        // 如果用户在页面选择执行任务的时候，传递参数进来了，这个时候就把任务参数设置到job中
        if (executorParam != null) {
            // 设置执行器的任务参数
            jobInfo.setExecutorParam(executorParam);
        }

        // 得到用户设定的该任务的失败重试次数
        int finalFailRetryCount = failRetryCount >= 0 ? failRetryCount : jobInfo.getExecutorFailRetryCount();
        // 根据JobGroup获取任务组，在生产环境中，一个定时任务不可能只有一个服务器在执行，对于相同的定时任务，注册到
        // xxl-job的服务器上时，会把相同定时任务的服务实例地址规整到一起，就赋值给XxlJobGroup这个类的addressList
        // 成员变量，不同的地址用逗号分隔
        XxlJobGroup group = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().load(jobInfo.getJobGroup());

        // 如果用户在web界面输入了执行器的地址，这里就会把执行器的地址设置到刚才查询到的执行器中
        if (addressList != null && addressList.trim().length() > 0) {
            // 这里设置执行器地址的注册方式，0 是自动注册，1 是用户手动注册
            group.setAddressType(1);
            // 然后把用户在 web 界面输入的执行器地址覆盖原来的执行器地址
            group.setAddressList(addressList.trim());
        }

        // ====== 处理分片广播的逻辑 ======
        // 定义一个分片数组
        int[] shardingParam = null;
        // 如果用户设定的分片参数不为null，其实这个参数一直是null，不会给用户设定的机会
        // 是程序内部根据用户是否配置了分片广播策略来自动设定分片参数的
        if (executorShardingParam != null) {
            // 如果参数不为null，那就讲字符串分割一下，分割成2个
            String[] shardingArr = executorShardingParam.split("/");
            // 做一下校验
            if (shardingArr.length == 2 && isNumeric(shardingArr[0]) && isNumeric(shardingArr[1])) {
                // 在这里初始化数组，容量为2数组的第一个参数就是分片序号，也就是代表的几号执行器，数组第二位就是总的分片数
                // 如果现在只有一台执行器在执行，那么数组一号位代表的就是0号执行器，2号位代表队就是只有一个分片，因为只有一个执行器
                shardingParam = new int[2];
                shardingParam[0] = Integer.valueOf(shardingArr[0]);
                shardingParam[1] = Integer.valueOf(shardingArr[1]);
            }
        }

        // ====== 分片广播的路由策略 ======
        if (ExecutorRouteStrategyEnum.SHARDING_BROADCAST == ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null)
                && group.getRegistryList() != null && !group.getRegistryList().isEmpty() && shardingParam == null) {
            // 如果配置了分片广播的路由策略，那就遍历执行器组，并且根据执行器组中的所有执行器地址集合的容量来遍历
            // 这也意味着有几个执行器，就得遍历几次
            for (int i = 0; i < group.getRegistryList().size(); i++) {
                // 既然是有几个执行器就要遍历几次，那正好就根据这个 i 去定义执行器在分片数组中的序号，如果是第一个被遍历到的执行器，就是 0 号执行器，以此类推
                // 而总的分片数不就是执行器组中存放执行器地址集合的长度吗？
                // 这里就会自动分片，然后告诉所有的执行器，让执行器去执行任务了，这里会出现一个问题，就是让所有的执行器都开始执行相同点定时任务，
                // 不会出现并发问题吗？理论上是会的，但是定时任务是程序员自己部署的，定时任务的逻辑也是程序员自己实现的，这就需要程序员自己在定时任务
                // 逻辑中把并发问题规避了，反正你能从定时任务中得到分片参数，能得到该定时任务具体是哪个分片序号
                processTrigger(group, jobInfo, finalFailRetryCount, triggerType, i, group.getRegistryList().size());
            }
        } else {
            // 如果没有配置分片策略，并且 executorShardingParam 数据为 null，那就直接使用默认的值
            // 说明只有一个执行器要执行任务
            if (shardingParam == null) {
                // 所以数组里面只有 0 和 1 两个元素
                shardingParam = new int[]{0, 1};
            }
            // 这里的 index 和 total 参数分别代表分片序号和分片总数的意思，如果只有一台执行器执行定时任务
            // 那分片序号为 0，分片总数为 1
            // 分片序号代表的是执行器，如果有 3 个执行器，那分片序号就是 0、1、2，分片总数就为 3
            // 在该方法之内，会真正开始远程调用，这个方法也是远程调用的核心方法
            processTrigger(group, jobInfo, finalFailRetryCount, triggerType, shardingParam[0], shardingParam[1]);
        }
    }

    /**
     * <h2>在该方法中会进一步处理分片和路由策略</h2>
     * <p>
     * 分片逻辑是程序内部自动处理好的，就是根据定时任务执行器的数量来自动分片，序号也是从小到大自动分配。
     * 而执行器的定时任务中可以获得分片的序号。
     * 分片广播的逻辑是在调度中心这一端实现的，调度中心实现的逻辑并不能保证同时调度的这些定时任务不会出现并发问题，
     * 要想解决可能出现的并发问题，就要在定时任务中编写具体的业务逻辑时动点脑子，把每个定时任务需要处理的数据分隔开。
     */
    private static void processTrigger(XxlJobGroup group, XxlJobInfo jobInfo, int finalFailRetryCount,
                                       TriggerTypeEnum triggerType, int index, int total) {
        // 获得定时任务的阻塞策略，默认是串行
        ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), ExecutorBlockStrategyEnum.SERIAL_EXECUTION);
        // 得到当前要调度的执行任务的路由策略，默认是没有
        ExecutorRouteStrategyEnum executorRouteStrategyEnum = ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null);
        // 判断路由策略是否等于分片广播，如果等于，就把分片参数拼接成字符串
        String shardingParam = (ExecutorRouteStrategyEnum.SHARDING_BROADCAST == executorRouteStrategyEnum) ? String.valueOf(index).concat("/").concat(String.valueOf(total)) : null;

        // ====== 1、定时任务日志处理 ======
        // 创建一个日志对象，用于记录该定时任务执行时的一些信息
        XxlJobLog jobLog = new XxlJobLog();
        // 记录定时任务的执行器组 ID
        jobLog.setJobGroup(jobInfo.getJobGroup());
        // 把定时任务的主键ID，设置到XxlJobLog日志对象中
        jobLog.setJobId(jobInfo.getId());
        // 设置定时任务的触发时间
        jobLog.setTriggerTime(new Date());
        // 把定时任务日志保存到数据库中，保存成功之后，定时任务日志的 ID 也就有了
        XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().save(jobLog);
        log.debug(">>>>>>>>>>> xxl-job trigger start, jobId:{}", jobLog.getId());

        // ====== 2、初始化触发器参数 ======
        // 构建触发器参数对象，这个是要在执行器那一端进行使用的
        TriggerParam triggerParam = new TriggerParam();
        // 设置任务 ID
        triggerParam.setJobId(jobInfo.getId());
        // 设置执行器要执行的任务方法名称
        triggerParam.setExecutorHandler(jobInfo.getExecutorHandler());
        // 把执行器要执行的任务参数设置进去
        triggerParam.setExecutorParams(jobInfo.getExecutorParam());
        // 把阻塞策略设置进去
        triggerParam.setExecutorBlockStrategy(jobInfo.getExecutorBlockStrategy());
        // 设置定时任务的超时时间
        triggerParam.setExecutorTimeout(jobInfo.getExecutorTimeout());
        // 设置定时任务的日志 ID
        triggerParam.setLogId(jobLog.getId());
        // 设置定时任务的触发时间，这个触发时间就是 jobLog 刚才设置的那个时间
        triggerParam.setLogDateTime(jobLog.getTriggerTime().getTime());
        // 设置执行模式，一般都是 BEAN 模式
        triggerParam.setGlueType(jobInfo.getGlueType());
        // 设置 glue 在线编辑的代码内容
        triggerParam.setGlueSource(jobInfo.getGlueSource());
        // 设置 glue 的更新时间
        triggerParam.setGlueUpdatetime(jobInfo.getGlueUpdatetime().getTime());
        // 设置分片参数
        triggerParam.setBroadcastIndex(index);
        triggerParam.setBroadcastTotal(total);

        // ====== 3、设定远程调用的服务实例地址 ======
        // 这里考虑到了路由策略
        String address = null;
        ReturnT<String> routeAddressResult = null;
        if (group.getRegistryList() != null && !group.getRegistryList().isEmpty()) {
            if (ExecutorRouteStrategyEnum.SHARDING_BROADCAST == executorRouteStrategyEnum) {
                // 如果是分片广播，就用分片数组中的参数选取对应的执行器地址
                if (index < group.getRegistryList().size()) {
                    address = group.getRegistryList().get(index);
                } else {
                    // 如果走到这里说明上面的索引超过集合长度了，就出错了，所以直接使用默认值 0 号索引
                    address = group.getRegistryList().get(0);
                }
            } else {
                // 走到这里说明不是分片广播，那就根据路由策略获得最终选用的执行器地址
                routeAddressResult = executorRouteStrategyEnum.getRouter().route(triggerParam, group.getRegistryList());
                if (routeAddressResult.getCode() == ReturnT.SUCCESS_CODE) {
                    address = routeAddressResult.getContent();
                }
            }
        } else {
            // 如果没得到地址，就赋值失败
            routeAddressResult = new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("jobconf_trigger_address_empty"));
        }

        // ====== 4、远程调用 ======
        ReturnT<String> triggerResult = null;
        // 地址不为空
        if (address != null) {
            // exec
            // 在这里进行远程调用，这里就是最核心远程调用的方法，但是方法内部的逻辑很简单，就是使用 HTTP 发送调用消息而已
            triggerResult = runExecutor(triggerParam, address);
        } else {
            triggerResult = new ReturnT<>(ReturnT.FAIL_CODE, null);
        }

        // ====== 5、拼接触发任务信息 ======
        StringBuffer triggerMsgSb = new StringBuffer();
        triggerMsgSb.append(I18nUtil.getString("jobconf_trigger_type")).append("：").append(triggerType.getTitle());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_admin_adress")).append("：").append(IpUtil.getIp());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regtype")).append("：").append((group.getAddressType() == 0) ? I18nUtil.getString("jobgroup_field_addressType_0") : I18nUtil.getString("jobgroup_field_addressType_1"));
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regaddress")).append("：").append(group.getRegistryList());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorRouteStrategy")).append("：").append(executorRouteStrategyEnum.getTitle());
        if (shardingParam != null) triggerMsgSb.append("(" + shardingParam + ")");
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorBlockStrategy")).append("：").append(blockStrategy.getTitle());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_timeout")).append("：").append(jobInfo.getExecutorTimeout());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorFailRetryCount")).append("：").append(finalFailRetryCount);
        triggerMsgSb.append("<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>" + I18nUtil.getString("jobconf_trigger_run") + "<<<<<<<<<<< </span><br>").append((routeAddressResult != null && routeAddressResult.getMsg() != null) ? routeAddressResult.getMsg() + "<br><br>" : "").append(triggerResult.getMsg() != null ? triggerResult.getMsg() : "");

        // ====== 6、保存触发信息的日志 ======
        // 设置执行器地址
        jobLog.setExecutorAddress(address);
        // 设置执行定时任务的方法名称
        jobLog.setExecutorHandler(jobInfo.getExecutorHandler());
        // 设置执行参数
        jobLog.setExecutorParam(jobInfo.getExecutorParam());
        // 设置分片参数
        jobLog.setExecutorShardingParam(shardingParam);
        // 设置失败重试次数
        jobLog.setExecutorFailRetryCount(finalFailRetryCount);
        // 设置触发结果码
        jobLog.setTriggerCode(triggerResult.getCode());
        // 设置触发任务信息，也就是调度备注
        jobLog.setTriggerMsg(triggerMsgSb.toString());
        // 更新数据库信息
        XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateTriggerInfo(jobLog);
        log.debug(">>>>>>>>>>> xxl-job trigger end, jobId:{}", jobLog.getId());
    }

    /**
     * <h2>该方法内部进行远程调用</h2>
     */
    public static ReturnT<String> runExecutor(TriggerParam triggerParam, String address) {
        ReturnT<String> runResult = null;
        try {
            // 获取一个用于远程调用的客户端对象，一个地址就对应着一个客户端，为什么说是客户端，因为远程调用的时候，执行器
            // 成为了服务器，因为执行器需要接收来自于客户端的调用消息
            ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
            // EXEC => ExecutorBizImpl#run {执行器内嵌服务根地址}/run
            // 拿到客户端后，就在 run 方法内进行远程调用了
            runResult = executorBiz.run(triggerParam);
        } catch (Exception e) {
            log.error(">>>>>>>>>>> xxl-job trigger error, please check if the executor[{}] is running.", address, e);
            runResult = new ReturnT<>(ReturnT.FAIL_CODE, ThrowableUtil.toString(e));
        }

        // 在这里拼接一下远程调用返回的状态码和消息
        StringBuffer runResultSB = new StringBuffer(I18nUtil.getString("jobconf_trigger_run") + "：");
        runResultSB.append("<br>address：").append(address);
        runResultSB.append("<br>code：").append(runResult.getCode());
        runResultSB.append("<br>msg：").append(runResult.getMsg());
        runResult.setMsg(runResultSB.toString());
        return runResult;
    }


    /**
     * <h2>判断字符串的内容是不是数字</h2>
     */
    private static boolean isNumeric(String str) {
        try {
            int result = Integer.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

}
