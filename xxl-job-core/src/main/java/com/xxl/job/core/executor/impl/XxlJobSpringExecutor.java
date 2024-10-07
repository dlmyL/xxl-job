package com.xxl.job.core.executor.impl;

import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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
 * 这个类是执行器服务开始执行的入口，该类的afterSingletonsInstantiated方法会在IOC容器中
 * 的所有单例BEAN初始化后被回调，该类的对象会在用户自己创建的XxlJobConfig配置类中，被当做
 * 一个Bean对象被注入到IOC容器中。
 */
@Slf4j
public class XxlJobSpringExecutor extends XxlJobExecutor
        implements ApplicationContextAware, SmartInitializingSingleton, DisposableBean {

    @Getter
    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        XxlJobSpringExecutor.applicationContext = applicationContext;
    }

    /**
     * 该方法就是SmartInitializingSingleton接口中定义的方法，该方法会在SpringBoot中的所有
     * 单例bean创建完成后被回调，这个时候，SpringBoot的容器已经获得了，所有的单例Bean也都创
     * 建好了，所以就可以在该方法内将创建好的封装定时任务的对象缓存到执行器自己的Map中。
     */
    @Override
    public void afterSingletonsInstantiated() {
        /*
        ==把用户定义好的定时任务注册到IJobHandler中==
        这里的applicationContext是由该类实现的ApplicationContextAware接口帮忙注入的，
        之所以需要它，是因为ApplicationContextAware可以得到所有初始化好的单例Bean。
         */
        initJobHandlerMethodRepository(applicationContext);

        // 创建Glue工厂，默认使用Spring模式的工厂
        GlueFactory.refreshInstance(1);

        try {
            // ==启动执行器==
            super.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 把用户定义好的定时任务注册到IJobHandler中，其实就是MethodJobHandler。
     */
    private void initJobHandlerMethodRepository(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            return;
        }

        /*
        获取IOC容器中所有初始化好的Bean的名字，这里的后两个参数都为boolean类型：
        第一个决定查到的对象是否允许为非单例的，默认false，意思为不获得非单例对象
        第二个意思是查找的对象是否允许为延迟初始化的，默认true，就是允许的意思
         */
        String[] beanDefinitionNames = applicationContext.getBeanNamesForType(Object.class, false, true);
        for (String beanDefinitionName : beanDefinitionNames) {
            Object bean = null;
            // 根据名称获取每一个Bean
            Lazy onBean = applicationContext.findAnnotationOnBean(beanDefinitionName, Lazy.class);
            if (onBean != null) {
                log.debug("xxl-job annotation scan, skip @Lazy Bean:{}", beanDefinitionName);
                continue;
            } else {
                bean = applicationContext.getBean(beanDefinitionName);
            }

            // 这里定义的变量就是用来收集Bean对象中添加了@XxlJob注解的方法了
            Map<Method, XxlJob> annotatedMethods = null;
            try {
                // 通过反射获取Bean对象中添加了@XxlJob注解的方法
                annotatedMethods = MethodIntrospector.selectMethods(bean.getClass(),
                        new MethodIntrospector.MetadataLookup<XxlJob>() {
                            @Override
                            public XxlJob inspect(Method method) {
                                // 在这里检查方法是否添加了@XxlJob注解
                                return AnnotatedElementUtils.findMergedAnnotation(method, XxlJob.class);
                            }
                        });
            } catch (Throwable ex) {
                log.error("xxl-job method-jobhandler resolve error for bean[" + beanDefinitionName + "].", ex);
            }
            // 如果结果为空，说明该Bean对象中没有方法添加@XxlJob注解
            if (annotatedMethods == null || annotatedMethods.isEmpty()) {
                continue;
            }

            // 遍历添加@XxlJob注解的方法
            for (Map.Entry<Method, XxlJob> methodXxlJobEntry : annotatedMethods.entrySet()) {
                // 得到目标方法
                Method executeMethod = methodXxlJobEntry.getKey();
                // 得到注解
                XxlJob xxlJob = methodXxlJobEntry.getValue();
                // ==将标注了@XxlJob注解的Bean注册到IJobHandler==
                registJobHandler(xxlJob, bean, executeMethod);
            }
        }
    }

    /**
     * 释放资源的方法，该方法会调用到父类的方法，在父类的方法中其实就是
     * 先停止执行器的Netty服务器，然后停止每一个工作的JobThread。
     */
    @Override
    public void destroy() {
        super.destroy();
    }
}
