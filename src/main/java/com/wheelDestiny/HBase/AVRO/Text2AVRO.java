package com.wheelDestiny.HBase.AVRO;

import com.wheelDestiny.HBase.MR.Base.BaseMR;
import com.wheelDestiny.HBase.Util.Utils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 读取text转为avro文件
 *
 */
public class Text2AVRO extends BaseMR {

    public static class Text2AVROMapper extends Mapper<LongWritable, Text, AvroKey, NullWritable>{

        Schema schema = null;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            schema = new Schema.Parser().parse(Text2AVRO.class.getResourceAsStream("/Text2AVRO.avsc"));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");

            if(line.length<5){
                //记录不符合要求的记录
                context.getCounter("badLine","count").increment(1L);
                return;
            }
            String aid = line[0];
            String pkgname = line[1];
            String uptimestr = line[2];
            String typestr = line[3];
            String country = line[4];

            System.out.println("aid       " + aid        );
            System.out.println("pkgname   " + pkgname    );
            System.out.println("uptimestr " + uptimestr  );
            System.out.println("typestr   " + typestr    );
            System.out.println("country   " + country    );
            System.out.println("------------------------");

            GenericRecord record = new GenericData.Record(schema);

            long uptime = 0;
            int type = 0;

            if(Utils.isNotEmpty(uptimestr)){
                uptime = Long.parseLong(uptimestr);
            }
            if(Utils.isNotEmpty(typestr)){
                type = Integer.parseInt(typestr);
            }

            record.put("aid",aid);
            record.put("pkgname",pkgname);
            record.put("uptime",uptime);
            record.put("type",type);
            record.put("country",country);

            //将avro格式写出
            context.write(new AvroKey<GenericRecord>(record),NullWritable.get());

        }
    }

    @Override
    public Job getJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, getJobNameWithTaskId());

        job.setJarByClass(Text2AVRO.class);
        job.setMapperClass(Text2AVROMapper.class);

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job,getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job,getJobOutputPath(getJobNameWithTaskId()));


        Schema schema = new Schema.Parser().parse(Text2AVRO.class.getResourceAsStream("/Text2AVRO.avsc"));

        AvroJob.setOutputKeySchema(job,schema);

        return job;
    }

    @Override
    public String getJobName() {
        return "text2avro";
    }
}
