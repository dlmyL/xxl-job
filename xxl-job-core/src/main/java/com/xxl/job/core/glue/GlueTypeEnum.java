package com.xxl.job.core.glue;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * <h1>任务模式</h1>
 */
@Getter
@AllArgsConstructor
public enum GlueTypeEnum {

    /**
     * 一般都是 BEAN 模式
     */
    BEAN("BEAN", false, null, null),
    GLUE_GROOVY("GLUE(Java)", false, null, null),
    GLUE_SHELL("GLUE(Shell)", true, "bash", ".sh"),
    GLUE_PYTHON("GLUE(Python)", true, "python", ".py"),
    GLUE_PHP("GLUE(PHP)", true, "php", ".php"),
    GLUE_NODEJS("GLUE(Nodejs)", true, "node", ".js"),
    GLUE_POWERSHELL("GLUE(PowerShell)", true, "powershell", ".ps1");

    private final String desc;
    private final boolean isScript;
    private final String cmd;
    private final String suffix;

    public static GlueTypeEnum match(String name){
        for (GlueTypeEnum item: GlueTypeEnum.values()) {
            if (item.name().equals(name)) {
                return item;
            }
        }
        return null;
    }

}
