package com.xxl.job.core.glue;

import com.xxl.job.core.glue.impl.SpringGlueFactory;
import com.xxl.job.core.handler.IJobHandler;
import groovy.lang.GroovyClassLoader;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * <h1>运行模式工厂类</h1>
 */
public class GlueFactory {

	/*
	XXL-Job 是一个分布式任务调度平台，而 GLUE 模式是 XXL-Job 中的一种执行模式，用于支持以 Java 代码形式编写和执行任务。
	GLUE 模式中，任务执行的代码逻辑由用户自己编写，并打包成一个可执行的 Jar 文件。XXL-Job 在调度时会将该 Jar 文件上传至执行器服务器，
	并通过 Java 的反射机制来执行指定的方法。这意味着任务的执行逻辑完全由用户自定义，可以处理任何复杂的业务逻辑需求。

	在 GLUE 模式中，用户需要自行编写任务代码，并且需要遵循特定的规范：
		创建一个类，类名必须为 com.xxl.job.core.glue.GlueJob。
		在类中实现 execute 方法，该方法将作为任务的执行入口。
		任务执行的参数由 XxlJobHelper 提供的静态方法获取，如 XxlJobHelper.getJobParam()。
		在 execute 方法中编写具体的任务逻辑代码，可以根据参数进行业务处理。
		编译打包任务代码，并生成可执行的 Jar 文件。

	使用 GLUE 模式的好处是灵活性高，用户可以根据自己的需求编写任意复杂的任务逻辑。同时，GLUE 模式也提供了一些辅助类和方法，用于方便任务的开发，
	如获取任务参数、记录任务日志等。
	需要注意的是，GLUE 模式要求用户自行管理任务的依赖和资源，包括 Jar 包的打包和上传，以及相关类或资源文件的引入和处理。

	总而言之，GLUE 模式是 XXL-Job 中一种基于 Java 代码编写和执行任务的模式，提供了灵活性和自定义性，适用于复杂的业务逻辑需求。
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


	/**
	 * groovy class loader
	 */
	private GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
	private ConcurrentMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();

	/**
	 * load new instance, prototype
	 *
	 * @param codeSource
	 * @return
	 * @throws Exception
	 */
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

	/**
	 * inject service of bean field
	 *
	 * @param instance
	 */
	public void injectService(Object instance) {
		// do something
	}

}
