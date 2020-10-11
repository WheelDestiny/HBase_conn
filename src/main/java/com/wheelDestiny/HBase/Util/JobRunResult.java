/**
 * JobRunResult.java
 * com.hainiuxy.mrrun.util
 * Copyright (c) 2018, 海牛版权所有.
 * @author   潘牛                      
*/

package com.wheelDestiny.HBase.Util;

import org.apache.hadoop.mapreduce.Counters;

import java.util.*;
import java.util.Map.Entry;

/**
 * 封装任务链上任务的执行结果
 * @author   潘牛                      
 * @Date	 2018年11月27日 	 
 */
public class JobRunResult {
	
	/**
	 * true:success; false:fail
	 */
	private boolean isSuccess;
	
	
	/**
	 * 任务链的运行时长，带有指定格式的，如 x天x小时x分x秒
	 */
	private String runningTime;
	
	
	/**
	 * 装失败任务名称的列表
	 */
	private List<String> failedJobNames = new ArrayList<String>();
	
	
	
	/**
	 * 装的是每个任务的counters
	 * key:jobname
	 * value:对应jobname的counters
	 */
	private Map<String, Counters> counterMap = new HashMap<String, Counters>();
	
	
	
	/**
	 * 打印一些运行结果
	 * @param isPrintCounter true:打印counter，false：不打印counter
	*/
	public void print(boolean isPrintCounter){
		StringBuilder sb = new StringBuilder();
		sb.append("任务链运行时长：").append(runningTime).append("\n");
		if(isSuccess){
			sb.append("任务链运行：SUCCESS").append("\n");
		}else{
			sb.append("任务链运行：FAIL").append("\n");
			sb.append("失败的任务名称：\n");
			for(String jobName : failedJobNames){
				sb.append(jobName).append("\n");
			}
		}
		
		if(isPrintCounter){
			sb.append("---------------------\ncounter统计\n");
			Set<Entry<String, Counters>> entrySet = this.counterMap.entrySet();
			for(Entry<String, Counters> entry : entrySet){
				String jobName = entry.getKey();
				Counters counters = entry.getValue();
				sb.append(jobName).append(":").append(counters).append("\n-------------\n");
			}
		}
		
		System.out.println(sb.toString());
	}





	public boolean isSuccess() {
		return isSuccess;
	}


	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}



	public String getRunningTime() {
		return runningTime;
	}


	public void setRunningTime(String runningTime) {
		this.runningTime = runningTime;
	}



	public List<String> getFailedJobNames() {
		return failedJobNames;
	}



	public void addFailedJobName(String failedJobName) {
		this.failedJobNames.add(failedJobName);
	}



	public Counters getCounters(String jobName) {
		return counterMap.get(jobName);
	}



	public void setCounters(String jobName, Counters counters) {
		this.counterMap.put(jobName, counters);
	}
	
	
	
	
}

