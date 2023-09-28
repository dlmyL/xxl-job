package com.xxl.job.admin.core.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * <h1>
 * 执行器组的实体类，实际上就是把执行相同定时任务的执行器用这个对象来封装，因为执行相同
 * 定时任务的执行器出了服务实例地址不同，其他的都相同，所以可以放到一起，服务实例地址用逗号
 * 隔开即可，该实体类对应的就是数据库中的 【xxl-job-group】这张表
 * </h1>
 */
@Data
public class XxlJobGroup {

    private int id;
    private String appname;         // 执行器中配置的项目名称
    private String title;           // 中文名称
    private int addressType;        // 执行器地址类型：0=自动注册、1=手动录入
    private String addressList;     // 执行器地址列表，多地址逗号分隔(手动录入)
    private Date updateTime;        // 更新时间

    // registry list
    private List<String> registryList;  // 执行器地址列表(系统注册)，这其实是把 addressList 变成 list 集合了

    /**
     * <h2>这个会将 addressList 属性中的所有地址转变成 list 集合</h2>
     */
    public List<String> getRegistryList() {
        if (addressList != null && addressList.trim().length() > 0) {
            registryList = new ArrayList<>(Arrays.asList(addressList.split(",")));
        }
        return registryList;
    }

}
