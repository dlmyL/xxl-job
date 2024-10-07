package com.xxl.job.core.handler.annotation;

import java.lang.annotation.*;

@Inherited                          // 描述子类可以继承父类的注解
@Target({ElementType.METHOD})       // 描述注解用在什么地方
@Retention(RetentionPolicy.RUNTIME) // 描述注解的生命周期
public @interface XxlJob {

    /**
     * 定时任务的名称
     */
    String value();

    /**
     * 初始化方法
     */
    String init() default "";

    /**
     * 销毁方法
     */
    String destroy() default "";
}
