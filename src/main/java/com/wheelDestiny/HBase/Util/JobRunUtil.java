/**
 * JobRunUtil.java
 * com.hainiuxy.mrrun.util
 * Copyright (c) 2018, 海牛版权所有.
 * @author   潘牛                      
*/

package com.wheelDestiny.HBase.Util;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 提交运行任务链和关闭任务链的工具类
 * @author   潘牛                      
 * @Date	 2018年11月27日 	 
 */
public class JobRunUtil {
	
	// 创建一个线程池
	private static ExecutorService service = Executors.newFixedThreadPool(1);
	
	public static JobRunResult run(JobControl jobc) throws Exception{
		//运行任务链
		new Thread(jobc).start(); 
		//创建带有返回值的多线程实例
		MonitorAndStopJobControlCallable callable = new MonitorAndStopJobControlCallable(jobc);
		// 线程池提交线程，并返回封装的返回对象
		Future<JobRunResult> futureTask = service.submit(callable);

		//通过FutrueTask实例.get()获取返回值
		return futureTask.get();
	}
	
	
	/**
	 * 带有返回值的线程
	 */
	public static class MonitorAndStopJobControlCallable  implements Callable<JobRunResult>{
		private JobControl jobc;
		
		public MonitorAndStopJobControlCallable(JobControl jobc) {
			this.jobc = jobc;
		}
		
		
		@Override
		public JobRunResult call() throws Exception {
			long startTime = System.currentTimeMillis();
			//循环判断任务链上的任务是否都完成
			while(!jobc.allFinished()){
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
			
					e.printStackTrace();
					
				}
			}
			//如果程序走到这，就代表任务链上任务完成了
			JobRunResult result = new JobRunResult();
			
			long endTime = System.currentTimeMillis();
			
			long runningTime = endTime - startTime;
			//设置运行时长
			result.setRunningTime(formatRunningTime(runningTime));
			
			//获取失败任务列表
			List<ControlledJob> failedJobList = jobc.getFailedJobList();
			//如果任务列表是空的，就代表都成功了
			if(failedJobList.isEmpty()){
				result.setSuccess(true);
			}else{
				result.setSuccess(false);
				//如果失败，打印失败任务列表
				for(ControlledJob cjob : failedJobList){
					result.addFailedJobName(cjob.getJobName());
				}
			}
			//装成功任务的counters
			List<ControlledJob> successfulJobList = jobc.getSuccessfulJobList();
			for(ControlledJob cjob : successfulJobList){
				String jobName = cjob.getJobName();
				Counters counters = cjob.getJob().getCounters();
				result.setCounters(jobName, counters);
			}
			
//			停止任务链
			jobc.stop();
			return result;
			
		}


		/**
		 * 将毫秒数转化成具体的字符串格式，如：x天x小时x分x秒
		 * @param runningTime 毫秒数
		 * @return 字符串
		*/
		private String formatRunningTime(long runningTime) {
			long day, hour,minute,second;
			day = runningTime / (1000 * 60 * 60 * 24);
			hour = runningTime % (1000 * 60 * 60 * 24) / (1000 * 60 * 60);
			minute = runningTime % (1000 * 60 * 60) / (1000 * 60);
			second = runningTime % (1000 * 60) / 1000;
//			System.out.println("day:" + day);
//			System.out.println("hour:" + hour);
//			System.out.println("minute:" + minute);
//			System.out.println("second:" + second);
			StringBuilder sb = new StringBuilder();
			if(day != 0){
				sb.append(day).append("天");
			}
			if(hour != 0){
				sb.append(hour).append("小时");
			}
			if(minute != 0){
				sb.append(minute).append("分");
			}
			if(second != 0){
				sb.append(second).append("秒");
			}
			return sb.toString();
			
		}
		
	}

}

