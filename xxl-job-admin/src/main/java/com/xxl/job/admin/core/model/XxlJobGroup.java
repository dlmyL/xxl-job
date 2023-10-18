package com.xxl.job.admin.core.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * <h1>
 * xxl-job的执行器是要把自身部署的服务器的信息发送给调度中心的，调度中心会把这些信息记录到数据库，
 * 当然也会把这些执行器的信息收集起来，把appName相同的执行器，封装到同一个Group中。
 *
 * 执行器组的实体类，实际上就是把执行相同定时任务的执行器用这个对象来封装，因为执行相同
 * 定时任务的执行器出了服务实例地址不同，其他的都相同，所以可以放到一起，服务实例地址用逗号
 * 隔开即可，该实体类对应的就是数据库中的 【xxl-job-group】这张表
 * </h1>
 */
@Data
public class XxlJobGroup {

    /*
    只要是appName相同的执行器的服务器信息，都会被封装到同一个XxlJobGroup对象中，并且该对象中的registryList成员变量，
    存放的就是这些执行器部署的服务器的IP地址。
    这样一来，当调度中心将要调度一个定时任务的时候，就可以根据定时任务信息中的jobGroup这个id，
    查询出定时任务究竟属于哪一个XxlJobGroup。
     */

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
