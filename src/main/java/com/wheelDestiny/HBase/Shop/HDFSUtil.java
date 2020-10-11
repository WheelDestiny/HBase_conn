/**
 * HDFSUtil.java
 * com.hainiu.utils
 * Copyright (c) 2020, 海牛版权所有.
 * @author   潘牛                      
*/

package com.wheelDestiny.HBase.Shop;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 操作hdfs的工具类
 * @author   潘牛                      
 * @Date	 2020年7月15日 	 
 */
public class HDFSUtil {
	
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");

	public static String getInputFiles(Configuration conf, String startTimeStr, String endTimeStr, Path inputBasePath)throws Exception{
		long startTime = 0, endTime = 0;
		
		// 获取前一小时的yyyyMMddHH
		Date date = new Date(new Date().getTime() - 1000 * 3600);
		String format = sdf.format(date);
		endTime = Long.parseLong(format);
		
		
		startTime = endTime;
		if(startTimeStr != null && !"".equals(startTimeStr.trim())){
			startTime = Long.valueOf(startTimeStr);
		}
		
		if(endTimeStr != null && !"".equals(endTimeStr.trim())){
			endTime = Long.valueOf(endTimeStr);
		}
		System.out.println("starttime:" + startTime);
		System.out.println("endtime:" + endTime);
		
		FileSystem fs = FileSystem.get(conf);
		TreeSet<String> set = new TreeSet<String>();
		listFile(fs, inputBasePath, set);
		StringBuilder sb = new StringBuilder();
		for(String p : set){
//			System.out.println(p);
			String[] split = p.split("/");
			String hour = split[split.length - 1];
			String day = split[split.length - 2];
			String year = split[split.length - 3];
			long time = Long.valueOf("" + year + day + hour);
//			System.out.println("includefilepathtime:" + time);
			if(time >= startTime && time <= endTime){
				sb.append(p).append(",");
			}
		}
		if(sb.length() != 0){
			sb.deleteCharAt(sb.length()-1);
		}
		
		
		return sb.toString();
	}
	
	/**
	 * 递归遍历输入目录，拿到有文件的输入目录，并装入set
	 * @param fs
	 * @param path
	 * @param set
	 * @throws Exception TODO(这里描述每个参数,如果有返回值描述返回值,如果有异常描述异常)
	*/
	public static void listFile(FileSystem fs, Path path, Set<String> set) throws Exception{
		if(fs.isFile(path)){
			return;
		}
		
		FileStatus[] listStatus = fs.listStatus(path);
		if(listStatus.length == 0){
//			System.out.println("====> " + path.toString());
			return;
		}
		for(FileStatus fileStatus : listStatus){
			if(fileStatus.isDirectory()){
				listFile(fs, fileStatus.getPath(), set);
				
			}else{
//				System.out.println("====> " + path.toString());
				set.add(path.toString());
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		String startTime = null;
		String endTime = null;
		Configuration conf = new Configuration();
		Path path = new Path("/tmp/hainiushop/hainiu_shop_service_all_log");
		String inputFiles = getInputFiles(conf, startTime, endTime, path);
		System.out.println(inputFiles);
	}
}

