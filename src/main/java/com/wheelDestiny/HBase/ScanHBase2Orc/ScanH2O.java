package com.wheelDestiny.HBase.ScanHBase2Orc;

import com.wheelDestiny.HBase.MR.Base.BaseMR;
import com.wheelDestiny.HBase.Util.OrcFormat;
import com.wheelDestiny.HBase.Util.OrcUtil;
import com.wheelDestiny.HBase.Util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ScanH2O extends BaseMR {

    /**
     *initTableMapperJob(table, scan, mapper, outputKeyClass,
     *               outputValueClass, job, addDependencyJars, TableInputFormat.class);
     * InputFormat<ImmutableBytesWritable, Result>
     * keyin:rowKey
     * valuein:一行的数据
     *
     * Orc
     *
     */
    public static class SH2OMapper extends TableMapper< NullWritable, Writable> {

        OrcUtil orcUtil = new OrcUtil();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //设置写的SCHEMA
            orcUtil.setOrcTypeWriteSchema(OrcFormat.SCHEMA);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String rowKey = Bytes.toString(key.get());
            String[] s = rowKey.split("_");
            String aid = s[0];

            String pkgname = Bytes.toString(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("pkgname")));
            String uptimestr = Bytes.toString(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("uptime")));
            String typestr = Bytes.toString(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("type")));
            String country = Bytes.toString(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("country")));
            String gpcategory = Bytes.toString(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("gpcategory")));

            System.out.println("aid       " + aid        );
            System.out.println("pkgname   " + pkgname    );
            System.out.println("uptimestr " + uptimestr  );
            System.out.println("typestr   " + typestr    );
            System.out.println("country   " + country    );
            System.out.println("gpcategory" + gpcategory );
            System.out.println("------------------------");


            Long uptime = 0L;
            int type = 0;
            if(Utils.isEmpty(uptimestr)){
                uptime = Long.parseLong(uptimestr);
            }
            if(Utils.isEmpty(typestr)){
                type = Integer.parseInt(typestr);
            }
            //struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>
            orcUtil.addAttr(aid,pkgname,uptime,type,country,gpcategory);

            Writable serialize = orcUtil.serialize();

            //写出Orc格式
            context.write(NullWritable.get(),serialize);
        }
    }

    @Override
    public Job getJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, getJobNameWithTaskId());

        job.setJarByClass(ScanH2O.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(OrcNewOutputFormat.class);

        TableName tableName = TableName.valueOf("wheeldestiny:userHBase");

        Scan scan = new Scan();

        //通过scan读取hbase表数据的配置，加载到job对象的conf中
        TableMapReduceUtil.initTableMapperJob(tableName,scan,SH2OMapper.class,NullWritable.class,Writable.class,job);

        FileOutputFormat.setOutputPath(job,getJobOutputPath(getJobNameWithTaskId()));

        return job;
    }

    @Override
    public String getJobName() {
        return "ScanH2O";
    }
}
