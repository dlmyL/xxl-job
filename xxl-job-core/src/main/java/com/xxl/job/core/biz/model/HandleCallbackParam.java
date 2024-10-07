package com.xxl.job.core.biz.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 这个就是用于执行器回调定时任务执行结果的包装类，执行结果的信息用这个类的对象封装
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HandleCallbackParam implements Serializable {

    private static final long serialVersionUID = 42L;

    private long logId;         // 这个就是定时任务的主键ID
    private long logDateTim;

    private int handleCode;    // 定时任务执行结果的状态码，成功还是失败
    private String handleMsg;

}
