package com.xxl.job.core.biz.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * <h1>注册执行器到调度中心时发送的注册参数</h1>
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
     * 执行器的注册名称
     */
    private String registryKey;
    /**
     * 执行器的地址
     */
    private String registryValue;

}
