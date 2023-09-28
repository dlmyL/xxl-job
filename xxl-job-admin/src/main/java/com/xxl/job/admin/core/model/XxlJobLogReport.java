package com.xxl.job.admin.core.model;

import lombok.Data;

import java.util.Date;

/**
 * <h1>日志报告对应的实体类</h1>
 */
@Data
public class XxlJobLogReport {

    private int id;
    private Date triggerDay;
    private int runningCount;
    private int sucCount;
    private int failCount;

}
