/**
 * WordMaxJob.java
 * com.hainiuxy.mrrun
 * Copyright (c) 2018, 海牛版权所有.
 * @author   潘牛                      
*/

package com.wheelDestiny.HBase.Shop;

import com.wheelDestiny.HBase.Util.JobRunResult;
import com.wheelDestiny.HBase.Util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 *  txt 文件转avro文件的任务链
 * @author   潘牛                      
 * @Date	 2018年9月29日 	 
 */
public class AccessTextToAvroJob extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		//通过getConf(),获取configuration，这样可以把-D参数传过来
		Configuration conf = getConf();
		
		//创建任务链JobControl对象
		JobControl jobc = new JobControl("AccessTextToAvroJob");
		
		AccessTextToAvro avro = new AccessTextToAvro();
		avro.setConf(conf);
		
		ControlledJob toAvroJob = avro.getControlledJob();
	
		jobc.addJob(toAvroJob);
		
		
		
		JobRunResult result = JobRunUtil.run(jobc);
		result.print(false);
		
		// 把counter写入MySQL
		
		return 0; 
		
	}



	public static void main(String[] args) throws Exception {
//		-Dtask.id=1031 -Dtask.input.dir=/tmp/etl/avro/input -Dtask.base.dir=/tmp/etl/avro
		System.exit(ToolRunner.run(new AccessTextToAvroJob(), args));
	}

}

