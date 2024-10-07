package com.xxl.job.core.handler.impl;

import com.xxl.job.core.handler.IJobHandler;

import java.lang.reflect.Method;

/**
 * 利用反射执行定时任务的方法
 */
public class MethodJobHandler extends IJobHandler {

    // 目标类对象，就是用户定义的IOC容器的Bean
    private final Object target;

    // Bean对象的初始化方法，对应#init
    private Method initMethod;

    // 目标方法，就是要被执行的定时任务方法，对应#execute
    private final Method method;

    // Bean对象的销毁方法，对应#destroy
    private Method destroyMethod;

    public MethodJobHandler(Object target, Method method,
                            Method initMethod, Method destroyMethod) {
        this.target = target;
        this.method = method;
        this.initMethod = initMethod;
        this.destroyMethod = destroyMethod;
    }

    /** 通过反射调用@XxlJob中的#init方法 */
    @Override
    public void init() throws Exception {
        if (initMethod != null) {
            initMethod.invoke(target);
        }
    }

    /** 通过反射执行定时任务的方法 */
    @Override
    public void execute() throws Exception {
        Class<?>[] paramTypes = method.getParameterTypes();
        if (paramTypes.length > 0) {
            method.invoke(target, new Object[paramTypes.length]);
        } else {
            method.invoke(target);
        }
    }


    /** 通过反射调用@XxlJob中的#destroy方法 */
    @Override
    public void destroy() throws Exception {
        if (destroyMethod != null) {
            destroyMethod.invoke(target);
        }
    }

    @Override
    public String toString() {
        return super.toString() + "[" + target.getClass() + "#" + method.getName() + "]";
    }
}
