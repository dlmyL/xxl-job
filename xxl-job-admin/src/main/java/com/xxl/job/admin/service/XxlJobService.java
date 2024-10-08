package com.xxl.job.admin.service;


import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.core.biz.model.ReturnT;

import java.util.Date;
import java.util.Map;

/**
 * core job action for xxl-job
 * 
 * @author xuxueli 2016-5-28 15:30:33
 */
public interface XxlJobService {

	Map<String, Object> pageList(int start, int length, int jobGroup, int triggerStatus,
								 String jobDesc, String executorHandler, String author);

	ReturnT<String> add(XxlJobInfo jobInfo);

	ReturnT<String> update(XxlJobInfo jobInfo);

	ReturnT<String> remove(int id);

	ReturnT<String> start(int id);

	ReturnT<String> stop(int id);

	Map<String,Object> dashboardInfo();

	ReturnT<Map<String,Object>> chartInfo(Date startDate, Date endDate);
}
