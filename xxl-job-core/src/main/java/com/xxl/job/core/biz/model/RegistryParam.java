package com.xxl.job.core.biz.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * 注册执行器到调度中心时发送的注册参数，封装要发送给调度中心的定时任务的信息
 */
@Data
@AllArgsConstructor
public class RegistryParam implements Serializable {

    private static final long serialVersionUID = 42L;

    /**
     * 注册方式
     */
    private String registryGroup;
    /**
     * 执行器的注册名称（定时任务方法的名称）
     * registryKey这个成员变量并不会被定时任务的方法名称赋值了，而是被执行器的唯一标识appName赋值
     * 定时任务真正的注册是在调度中心的web模块由用户手动输入的，将要注册的定时任务和执行器绑定起来
     */
    private String registryKey;
    /**
     * 执行器的地址（定时任务程序部署的服务器的ip地址）
     */
    private String registryValue;
}
