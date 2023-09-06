package com.xxl.job.core.executor.impl;

import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * <h1>这个类就是执行器服务开始执行的入口，该类的 afterSingletonsInstantiated 方法会在 IOC 容器中
 * 的所有单例 BEAN 初始化后被回调，该类的对象会在用户自己创建的 XxlJobConfig 配置类中，被当做一个 Bean 对象被注入到IOC容器中</h1>
 *
 * @author xuxueli 2018-11-01 09:24:52
 */
public class XxlJobSpringExecutor extends XxlJobExecutor implements ApplicationContextAware, SmartInitializingSingleton, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobSpringExecutor.class);

    /**
     * <h2>执行器启动的入口，在该方法内会把用户定义的定时任务，也就是 @XxlJob 注解的方法注册到 IJobHandler</h2>
     */
    @Override
    public void afterSingletonsInstantiated() {

        /*
            该方法就会把用户定义的所有定时任务注册到 IJobHandler 中，这里的 applicationContext 是由
            该类实现的 ApplicationContextAware 接口帮忙注入的，这所以需要它，是因为 ApplicationContextAware
            可以得到所有初始化好的单例 BEAN
         */
        initJobHandlerMethodRepository(applicationContext);

        // 创建 glue 工厂，默认使用 Spring 模式的工厂
        GlueFactory.refreshInstance(1);

        // 在这里调用父类的方法启动了执行器
        try {
            super.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * <h2>
     *     释放资源的方法，该方法会调用到父类的方法，在父类的方法中其实就是停止了执行器的 Netty 服务器，
     *     停止了每一个工作的 JobThread 线程
     * </h2>
     */
    @Override
    public void destroy() {
        super.destroy();
    }

    /**
     * <h3>
     *     该方法会把用户定义的所有定时任务注册到 IJobHandler 对象中，其实是 MethodJobHandler 对象，
     *     MethodJobHandler 对象是 IJobHandler 的子类
     * </h3>
     */
    private void initJobHandlerMethodRepository(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            return;
        }
        /*
            获取 IOC 容器中所有初始化好的 BEAN 的名字，这里的后两个参数都为 boolean 类型
                第一个决定查到的对象是否允许为非单例的，传入 false，意思为不获得非单例对象
                第二个意思是查找的对象是否允许为延迟初始化的，就是LazyInit的意思，参数为true，就是允许的意思
         */
        String[] beanDefinitionNames = applicationContext.getBeanNamesForType(Object.class, false, true);
        for (String beanDefinitionName : beanDefinitionNames) {
            // 根据名称获取每一个 BEAN
            Object bean = null;
            Lazy onBean = applicationContext.findAnnotationOnBean(beanDefinitionName, Lazy.class);
            if (onBean!=null){
                logger.debug("xxl-job annotation scan, skip @Lazy Bean:{}", beanDefinitionName);
                continue;
            }else {
                bean = applicationContext.getBean(beanDefinitionName);
            }

            // 这里定义的变量就是用来收集 BEAN 对象中添加了 @XxlJob 注解的方法了
            Map<Method, XxlJob> annotatedMethods = null;
            try {
                // 通过反射获取 BEAN 对象中添加了 @XxlJob 注解的方法
                annotatedMethods = MethodIntrospector.selectMethods(bean.getClass(),
                        new MethodIntrospector.MetadataLookup<XxlJob>() {
                            @Override
                            public XxlJob inspect(Method method) {
                                // 在这里检查方法是否添加了 @XxlJob 注解
                                return AnnotatedElementUtils.findMergedAnnotation(method, XxlJob.class);
                            }
                        });
            } catch (Throwable ex) {
                logger.error("xxl-job method-jobhandler resolve error for bean[" + beanDefinitionName + "].", ex);
            }
            // 如果结果为空，说明该 BEAN 对象中没有方法添加了 @XxlJob 注解
            if (annotatedMethods==null || annotatedMethods.isEmpty()) {
                continue;
            }

            // 遍历添加 @XxlJob 注解的方法
            for (Map.Entry<Method, XxlJob> methodXxlJobEntry : annotatedMethods.entrySet()) {
                // 得到该方法
                Method executeMethod = methodXxlJobEntry.getKey();
                // 得到注解
                XxlJob xxlJob = methodXxlJobEntry.getValue();
                // 在这里将该方法注册到 JobHandler 的子类对象中，这个时候逻辑就会跑到父类了
                registJobHandler(xxlJob, bean, executeMethod);
            }

        }
    }


    // ---------------------- applicationContext ----------------------

    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        XxlJobSpringExecutor.applicationContext = applicationContext;
    }

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

}
