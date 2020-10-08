package com.wheelDestiny.HBase.AVRO;

import com.wheelDestiny.HBase.MR.Base.BaseMR;
import com.wheelDestiny.HBase.Util.OrcFormat;
import com.wheelDestiny.HBase.Util.OrcUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AVRO2Orc extends BaseMR {

    public static class AVRO2OrcMapper extends Mapper<AvroKey<GenericRecord>, NullWritable,NullWritable, Writable>{

        OrcUtil orcUtil = new OrcUtil();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            orcUtil.setOrcTypeWriteSchema(OrcFormat.SCHEMA2);
        }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            GenericRecord datum = key.datum();
            String aid = (String) datum.get("aid");
            String pkgname = (String) datum.get("pkgname");
            long uptime = (long) datum.get("uptime");
            int type = (int) datum.get("type");
            String country = (String) datum.get("country");

            System.out.println("aid       " + aid        );
            System.out.println("pkgname   " + pkgname    );
            System.out.println("uptime " + uptime  );
            System.out.println("type   " + type    );
            System.out.println("country   " + country    );
            System.out.println("------------------------");

            //写ORC
            orcUtil.addAttr(aid,pkgname,uptime,type,country);

            Writable serialize = orcUtil.serialize();

            context.write(NullWritable.get(),serialize);
        }
    }

    @Override
    public Job getJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, getJobNameWithTaskId());

        job.setJarByClass(AVRO2Orc.class);

        job.setMapperClass(AVRO2OrcMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Writable.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);

        Text2AVRO text2AVRO = new Text2AVRO();

        FileInputFormat.addInputPath(job,text2AVRO.getJobOutputPath(text2AVRO.getJobNameWithTaskId()));

        FileOutputFormat.setOutputPath(job,getJobOutputPath(getJobNameWithTaskId()));


        Schema schema = new Schema.Parser().parse(Text2AVRO.class.getResourceAsStream("/Text2AVRO.avsc"));

        AvroJob.setOutputKeySchema(job,schema);

        return job;
    }

    @Override
    public String getJobName() {
        return "AVRO2Orc";
    }
}
