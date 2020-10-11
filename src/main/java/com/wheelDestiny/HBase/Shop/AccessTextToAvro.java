/**
 * AccessTextToAvro.java
 * com.hainiuxy.etl
 * Copyright (c) 2018, 海牛版权所有.
 * @author   潘牛                      
*/

package com.wheelDestiny.HBase.Shop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.regex.Pattern;

import com.wheelDestiny.HBase.MR.Base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * @author   潘牛                      
 * @Date	 2018年10月31日 	 
 */
public class AccessTextToAvro extends BaseMR {
	
	
	public static class AccessTextToAvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable>{
		
		static SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		
		AvroMultipleOutputs avroMoutputs = null;
		
		Schema schema = null;
		
		String fileBasePath = null;
		@Override
		protected void setup(Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable>.Context context)
				throws IOException, InterruptedException {
			schema = new Schema.Parser().parse(AccessTextToAvro.class.getResourceAsStream("/dwd_shop_access.avsc"));				

			avroMoutputs = new AvroMultipleOutputs(context);
			
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			String path = inputSplit.getPath().toString();
			String[] split = path.split("/");
			String year = split[split.length-4];
			String day = split[split.length-3];
			String hour = split[split.length-2];
//			System.out.println("=====>" + year + day + hour);
			fileBasePath = year + day + hour;
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String[] split = value.toString().split("\001");
			
			// 需要统计总记录数
			if(! checkValid(split)){
				// 需要在这里统计 不符合条件的记录数
				return;
			}
			
			//统计有效的数据个数
//			---post
//			0)180.76.237.209
//			1)-
//			2)14/Dec/2017:02:43:36 +0800
//			3)POST /index.php HTTP/1.1
//			4)500
//			5)4571
//			6)siteid=1&modelid=11&username=kfc211784
//			7)-
//			8)Mozilla/4.0 (compatible; Win32; WinHttp.WinHttpRequest.5)
//			9)-
//			10)c3d7d6b6b0644d3f992439e0c859332d
//			11)pan123
//			System.out.println("===>" + value.toString());
            String ip = split[0];
            String timeStr = split[2];
            String request = split[3];
            String status = split[4];
            String request_body = split[6];
            String refer = split[7];
            String agent = split[8];
            String cookie = split[10];
            String userName = split[11];

            long longIP = IPUtil.ip2long(ip);
            long time = 0L;
            try {
            	timeStr = timeStr.substring(0, timeStr.indexOf(" "));
          
            	// 得到1970年到现在的秒数
				time = sdf.parse(timeStr).getTime() / 1000;
			} catch (ParseException e) {
				
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			}
            String[] requests = request.split(" ");
            String requestType = requests[0];
            String requestURL = requests[1];

            HashMap<String, String> requestAttr = new HashMap<>();
//            System.out.println("request_body======>" + request_body);
//            System.out.println("requestUrl======>" + requestURL);
            if (requestURL.indexOf("?") > 0) {
                String urlAttr = requestURL.substring(requestURL.indexOf("?") + 1);
                String[] kvs = urlAttr.split("&");
                for (String kv : kvs) {
                    String[] split1 = kv.split("=");
                    if(split1.length == 2){
//                    	System.out.println("requestUrl======>" + requestURL);
                      String k = split1[0];
                      String v = split1[1];
                      requestAttr.put(k, v);
                    }

                }
            }
            if (!request_body.equals("-")) {
                String[] kvs = request_body.split("&");
                for (String kv : kvs) {
                    String[] split1 = kv.split("=");
                    if(split1.length == 2){
//                    	System.out.println("request_body======>" + request_body);
                      String k = split1[0];
                      String v = split1[1];
                      requestAttr.put(k, v);
                    }

                }
            }
            
//			System.out.println("ip:" + longIP);
//			System.out.println("time:" + time);
//			System.out.println("requestType:" + requestType);
//			System.out.println("requestURL:" + requestURL);
//			System.out.println("status:" + status);
//			System.out.println("refer:" + refer);
//			System.out.println("agent:" + agent);
//			System.out.println("requestAttr:" + requestAttr);
//			System.out.println("cookie:" + cookie);
//			System.out.println("logonName:" + userName);
            
            GenericRecord record = new GenericData.Record(schema);
			record.put("ip", longIP);
			record.put("time", time);
			record.put("requestType", requestType);
			record.put("requestURL", requestURL);
			record.put("status", status);
			record.put("refer", refer);
			record.put("agent", agent);
			record.put("requestAttr", requestAttr);
			record.put("cookie", cookie); // a6a597b4f3b24e16ba87cec919a4162c 每个用户浏览器生成一个
			record.put("logonName", userName);
            
            // 输出avro的多目录输出
			avroMoutputs.write("dwdshopaccess",
					new AvroKey<GenericRecord>(record), 
					NullWritable.get(),
					"dwd_shop_access/" + fileBasePath + "/" + "access"
					);
            
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			avroMoutputs.close();
			
		}
		/**
		 * 校验日志是否是有效数据，如果不是，返回false
		 * @param splits
		 * @return 
		*/
		private boolean checkValid(String[] splits) {
			if (splits.length != 12){
				return false;
			}
			
			String req = splits[3];
			
			if(req == null){
				return false;
			}
			
			if(!req.contains("GET") && !req.contains("POST")){
				return false;
			}
//			过滤图片
			if(req.toLowerCase().contains(".jpg") || 
					req.toLowerCase().contains(".png") ||
					req.toLowerCase().contains(".gif")){

				return false;
			}
//			过滤其他不必要请求
			boolean filterFlag = Pattern.matches("(^GET //member|^GET /member|^GET /help|GET"
					+ " /js).*", req);
			
			if(filterFlag){

				return false;
			}
			return true;
			
		}
	}
	
	@Override
	public Job getJob(Configuration conf) throws IOException {
		
		Job job = Job.getInstance(conf, getJobNameWithTaskId());
		
		job.setJarByClass(AccessTextToAvro.class);
		
		job.setMapperClass(AccessTextToAvroMapper.class);
		
		//无reduce
		job.setNumReduceTasks(0);
		
		
		job.setMapOutputKeyClass(AvroKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(NullWritable.class);
		
		//【重要】avro输出format
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		
		//根据配置的avro.txt 配置文件，获取对应的schema对象
		Schema schema = new Schema.Parser().parse(AccessTextToAvro.class.getResourceAsStream("/dwd_shop_access.avsc"));

		// 设置多目录输出
		AvroMultipleOutputs.addNamedOutput(job, "dwdshopaccess", AvroKeyOutputFormat.class, schema);
		
		AvroJob.setOutputKeySchema(job, schema);
		String startTime = conf.get("start.time");
		String endTime = conf.get("end.time");
		String inputFiles = "";
		try {
			inputFiles = HDFSUtil.getInputFiles(conf, startTime, endTime, getFirstJobInputPath());
		} catch (Exception e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(inputFiles.length() == 0){
			System.out.println("input files is empty,exit!");
			System.exit(0);
		}
		System.out.println("inputfiles:" + inputFiles);
		
		FileInputFormat.addInputPaths(job, inputFiles);
		
		FileOutputFormat.setOutputPath(job, getJobOutputPath(getJobNameWithTaskId()));
		
		
		return job;
		
	}

	@Override
	public String getJobName() {
		
		return "access_txt2avro";
		
	}

}

