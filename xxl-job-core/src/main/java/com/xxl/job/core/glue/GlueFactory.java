package com.xxl.job.core.glue;

import com.xxl.job.core.glue.impl.SpringGlueFactory;
import com.xxl.job.core.handler.IJobHandler;
import groovy.lang.GroovyClassLoader;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 运行模式工厂类
 */
public class GlueFactory {

	/*
	XXL-JOB是一个分布式任务调度平台，而GLUE模式是XXL-JOB中的一种执行模式，用于支持以Java代码形式编写和执行任务。
	GLUE模式中，任务执行的代码逻辑由用户自己编写，并打包成一个可执行的Jar文件。XXL-JOB在调度时会将该Jar文件上传至
	执行器服务器，并通过Java的反射机制来执行指定的方法。这意味着任务的执行逻辑完全由用户自定义，可以处理任何复杂的
	业务逻辑需求。

	在GLUE模式中，用户需要自行编写任务代码，并且需要遵循特定的规范：
		1、创建一个类，类名必须为 com.xxl.job.core.glue.GlueJob
		2、在类中实现execute方法，该方法将作为任务的执行入口
		3、任务执行的参数由XxlJobHelper提供的静态方法获取，如XxlJobHelper.getJobParam()
		4、在execute方法中编写具体的任务逻辑代码，可以根据参数进行业务处理
		5、编译打包任务代码，并生成可执行的Jar文件

	使用GLUE模式的好处是灵活性高，用户可以根据自己的需求编写任意复杂的任务逻辑。同时，GLUE模式也提供了一些辅助类和
	方法，用于方便任务的开发，如获取任务参数、记录任务日志等。
	需要注意的是，GLUE模式要求用户自行管理任务的依赖和资源，包括Jar包的打包和上传，以及相关类或资源文件的引入和处理。

	总而言之，GLUE模式是XXL-JOB中一种基于Java代码编写和执行任务的模式，提供了灵活性和自定义性，适用于复杂的业务逻辑需求。
	 */

	private static GlueFactory glueFactory = new GlueFactory();
	public static GlueFactory getInstance(){
		return glueFactory;
	}
	public static void refreshInstance(int type){
		if (type == 0) {
			glueFactory = new GlueFactory();
		} else if (type == 1) {
			glueFactory = new SpringGlueFactory();
		}
	}


	private GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
	private ConcurrentMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();

	public IJobHandler loadNewInstance(String codeSource) throws Exception{
		if (codeSource!=null && codeSource.trim().length()>0) {
			Class<?> clazz = getCodeSourceClass(codeSource);
			if (clazz != null) {
				Object instance = clazz.newInstance();
				if (instance!=null) {
					if (instance instanceof IJobHandler) {
						this.injectService(instance);
						return (IJobHandler) instance;
					} else {
						throw new IllegalArgumentException(">>>>>>>>>>> xxl-glue, loadNewInstance error, "
								+ "cannot convert from instance["+ instance.getClass() +"] to IJobHandler");
					}
				}
			}
		}
		throw new IllegalArgumentException(">>>>>>>>>>> xxl-glue, loadNewInstance error, instance is null");
	}

	private Class<?> getCodeSourceClass(String codeSource){
		try {
			// md5
			byte[] md5 = MessageDigest.getInstance("MD5").digest(codeSource.getBytes());
			String md5Str = new BigInteger(1, md5).toString(16);

			Class<?> clazz = CLASS_CACHE.get(md5Str);
			if(clazz == null){
				clazz = groovyClassLoader.parseClass(codeSource);
				CLASS_CACHE.putIfAbsent(md5Str, clazz);
			}
			return clazz;
		} catch (Exception e) {
			return groovyClassLoader.parseClass(codeSource);
		}
	}

	public void injectService(Object instance) {
		// do something
	}

}
