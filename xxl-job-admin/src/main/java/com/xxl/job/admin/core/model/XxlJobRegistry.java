package com.xxl.job.admin.core.model;

import java.util.Date;

/**
 * <h1>调度中心持有注册过来的执行器的实体类</h1>
 *
 * Created by xuxueli on 16/9/30.
 */
public class XxlJobRegistry {
    // 执行器 ID
    private int id;
    // 执行器的注册方法，是手动还是自动
    private String registryGroup;
    // 执行器的 appName
    private String registryKey;
    // 执行器的地址
    private String registryValue;
    // 更新时间
    private Date updateTime;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getRegistryGroup() {
        return registryGroup;
    }

    public void setRegistryGroup(String registryGroup) {
        this.registryGroup = registryGroup;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public void setRegistryKey(String registryKey) {
        this.registryKey = registryKey;
    }

    public String getRegistryValue() {
        return registryValue;
    }

    public void setRegistryValue(String registryValue) {
        this.registryValue = registryValue;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
