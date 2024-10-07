package com.xxl.job.admin.dao;

import com.xxl.job.admin.core.model.XxlJobInfo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * job info
 * @author xuxueli 2016-1-12 18:03:45
 */
@Mapper
public interface XxlJobInfoDao {

	List<XxlJobInfo> pageList(@Param("offset") int offset,
							  @Param("pagesize") int pagesize,
							  @Param("jobGroup") int jobGroup,
							  @Param("triggerStatus") int triggerStatus,
							  @Param("jobDesc") String jobDesc,
							  @Param("executorHandler") String executorHandler,
							  @Param("author") String author);

	int pageListCount(@Param("offset") int offset,
					  @Param("pagesize") int pagesize,
					  @Param("jobGroup") int jobGroup,
					  @Param("triggerStatus") int triggerStatus,
					  @Param("jobDesc") String jobDesc,
					  @Param("executorHandler") String executorHandler,
					  @Param("author") String author);
	
	int save(XxlJobInfo info);

	XxlJobInfo loadById(@Param("id") int id);
	
	int update(XxlJobInfo xxlJobInfo);
	
	int delete(@Param("id") long id);

	List<XxlJobInfo> getJobsByGroup(@Param("jobGroup") int jobGroup);

	int findAllCount();

	/**
	 * 根据执行时间查询定时任务信息的方法，这里查询的依据就是定时任务
	 * 下一次的执行时间。
	 * 比如当前时间是0秒，要查询10秒以内的可以执行的定时任务，那么就
	 * 判断定时任务下一次的执行时间只要是小于10秒的，都返回给用户，
	 * 这些定时任务都是在10秒内可以执行的。
	 */
	List<XxlJobInfo> scheduleJobQuery(@Param("maxNextTime") long maxNextTime,
									  @Param("pagesize") int pagesize );

	int scheduleUpdate(XxlJobInfo xxlJobInfo);
}
