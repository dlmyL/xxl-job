package com.xxl.job.core.biz.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * @author xuxueli 2020-04-11 22:27
 */
@Data
@AllArgsConstructor
public class LogParam implements Serializable {

    private static final long serialVersionUID = 42L;

    private long logDateTim;
    private long logId;
    private int fromLineNum;

}