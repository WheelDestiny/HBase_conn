package com.wheelDestiny.HBase.ReadSnapShot;

import com.wheelDestiny.HBase.MR.Base.BaseMR;
import com.wheelDestiny.HBase.Util.Constants;
import com.wheelDestiny.HBase.Util.OrcFormat;
import com.wheelDestiny.HBase.Util.OrcUtil;
import com.wheelDestiny.HBase.Util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReadSS extends BaseMR {

    public static class ReadSSMapper extends Mapper<ImmutableBytesWritable, Result, NullWritable, Writable>{
        OrcUtil orcUtil = new OrcUtil();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            orcUtil.setOrcTypeWriteSchema(OrcFormat.SCHEMA);

        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            String rowkey = Bytes.toString(key.get());
            String[] arr = rowkey.split("_");
            String aid = arr[0];

            String pkgname = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("pkgname")));
            String uptimestr = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("uptime")));
            String typestr = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("type")));
            String country = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("country")));
            String gpcategory = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("gpcategory")));

            System.out.println("aid       " + aid        );
            System.out.println("pkgname   " + pkgname    );
            System.out.println("uptimestr " + uptimestr  );
            System.out.println("typestr   " + typestr    );
            System.out.println("country   " + country    );
            System.out.println("gpcategory" + gpcategory );
            System.out.println("------------------------");

            long upTime = 0L;
            int type = 0;
            if(Utils.isNotEmpty(uptimestr)){
                upTime = Long.parseLong(uptimestr);
            }
            if(Utils.isNotEmpty(typestr)){
                type = Integer.parseInt(typestr);
            }
            // 写orc
//			struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>
            orcUtil.addAttr(aid, pkgname, upTime, type, country, gpcategory);

            Writable serialize = orcUtil.serialize();

            context.write(NullWritable.get(),serialize);
        }
    }

    @Override
    public Job getJob(Configuration conf) throws IOException {
        Configuration entries = HBaseConfiguration.create(conf);

        Job job = Job.getInstance(entries, getJobNameWithTaskId());

        job.setJarByClass(ReadSS.class);
        job.setMapperClass(ReadSSMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Writable.class);

        job.setNumReduceTasks(0);

        //设置读入快照的inputformat
        job.setInputFormatClass(TableSnapshotInputFormat.class);
        //设置输出orc
        job.setOutputFormatClass(OrcNewOutputFormat.class);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("8d303"));

        //设置筛选范围
        job.getConfiguration().set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
//        job.getConfiguration().set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));

        //获取快照路径
        String snapshotName = conf.get(Constants.HBASE_TABLE_SNAPSHOT_NAME_ATTR);

        //读取快照所需要的临时目录
        Path path = new Path(conf.get(Constants.HBASE_TABLE_SNAPSHOT_RESTORE_PATH_ATTR));

        TableSnapshotInputFormat.setInput(job,snapshotName,path);

        FileOutputFormat.setOutputPath(job,getJobOutputPath(getJobNameWithTaskId()));

        return job;
    }

    @Override
    public String getJobName() {
        return "ReadSnapshot";
    }
}
