package com.xxl.job.core.biz.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by xuxueli on 17/3/23.
 */
@Data
@AllArgsConstructor
public class LogResult implements Serializable {

    private static final long serialVersionUID = 42L;

    private int fromLineNum;
    private int toLineNum;
    private String logContent;
    private boolean isEnd;

}
