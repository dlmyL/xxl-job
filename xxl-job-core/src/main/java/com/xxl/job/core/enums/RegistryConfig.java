package com.xxl.job.core.enums;

/**
 * <h1>注册默认参数配置</h1>
 *
 * Created by xuxueli on 17/5/10.
 */
public class RegistryConfig {

    /**
     * 执行器注册到调度中心的心跳时间，其实就是 30s 重新注册一次，刷新注册时间，
     * 以防止调度中心主观任务执行器下线了
     */
    public static final int BEAT_TIMEOUT = 30;
    /**
     * 90s 就会自动踢下线
     */
    public static final int DEAD_TIMEOUT = BEAT_TIMEOUT * 3;

    /**
     * 注册类型
     */
    public enum RegistType{ EXECUTOR, ADMIN }

}
