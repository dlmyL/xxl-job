package com.xxl.job.core.handler.impl;

import com.xxl.job.core.handler.IJobHandler;

import java.lang.reflect.Method;

/**
 * <h1>反射调用定时任务</h1>
 *
 * @author xuxueli 2019-12-11 21:12:18
 */
public class MethodJobHandler extends IJobHandler {

    /**
     * 目标类对象，就是用户定义的 IOC 容器的 BEAN
     */
    private final Object target;
    /**
     * 目标方法，就是要被执行的定时任务方法
     */
    private final Method method;
    /**
     * BEAN 对象的初始化方法
     */
    private Method initMethod;
    /**
     * BEAN 对象的销毁方法
     */
    private Method destroyMethod;

    public MethodJobHandler(Object target, Method method, Method initMethod, Method destroyMethod) {
        this.target = target;
        this.method = method;
        this.initMethod = initMethod;
        this.destroyMethod = destroyMethod;
    }

    /**
     * <h2>通过反射执行定时任务的方法</h2>
     */
    @Override
    public void execute() throws Exception {
        Class<?>[] paramTypes = method.getParameterTypes();
        if (paramTypes.length > 0) {
            method.invoke(target, new Object[paramTypes.length]);       // method-param can not be primitive-types
        } else {
            method.invoke(target);
        }
    }

    /**
     * <h2>通过反射调用目标对象的 init 方法</h2>
     */
    @Override
    public void init() throws Exception {
        if(initMethod != null) {
            initMethod.invoke(target);
        }
    }

    /**
     * <h2>通过反射调用目标对象的 destroy 方法</h2>
     */
    @Override
    public void destroy() throws Exception {
        if(destroyMethod != null) {
            destroyMethod.invoke(target);
        }
    }

    @Override
    public String toString() {
        return super.toString()+"["+ target.getClass() + "#" + method.getName() +"]";
    }

}
