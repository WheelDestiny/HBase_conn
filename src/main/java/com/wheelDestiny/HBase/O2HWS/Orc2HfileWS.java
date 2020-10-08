package com.wheelDestiny.HBase.O2HWS;

import com.wheelDestiny.HBase.MR.Base.BaseMR;
import com.wheelDestiny.HBase.Util.Constants;
import com.wheelDestiny.HBase.Util.OrcFormat;
import com.wheelDestiny.HBase.Util.OrcUtil;
import com.wheelDestiny.HBase.Util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Orc2HfileWS extends BaseMR {

    /**
     *读取Orc文件
     * OrcNewInputFormat来确定输入kv
     *
     * 输出为HFile
     * ImmutableBytesWritable   rowKey
     * Put                      要写入的行数据
     *
     * reducer用自带的PutSortReducer
     */
    public static class Orc2HfileWSMapper extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put>{

        //创建ORCUtil对象
        OrcUtil orcUtil = new OrcUtil();

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

        ImmutableBytesWritable k = new ImmutableBytesWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            orcUtil设置读文件格式
            orcUtil.setOrcTypeReadSchema(OrcFormat.SCHEMA);
        }

        //读取ORC数据
        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            //读取orc数据
            //struct文件结构
            //SCHEMA = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>"
            String aid = orcUtil.getOrcData(value, "aid");
            String pkgname = orcUtil.getOrcData(value, "pkgname");
            String uptimestr = orcUtil.getOrcData(value, "uptime");
            String typestr = orcUtil.getOrcData(value, "type");
            String country = orcUtil.getOrcData(value, "country");
            String gpcategory = orcUtil.getOrcData(value, "gpcategory");

//            System.out.println("aid       " + aid        );
//            System.out.println("pkgname   " + pkgname    );
//            System.out.println("uptimestr " + uptimestr  );
//            System.out.println("typestr   " + typestr    );
//            System.out.println("country   " + country    );
//            System.out.println("gpcategory" + gpcategory );
//            System.out.println("------------------------");

            //写入HBase
            //设计rowKey： aid_yyyyMMdd
            long time = Long.parseLong(uptimestr) * 1000;
            Date date = new Date(time);
            String day = format.format(date);

            StringBuilder rowKey = new StringBuilder();
            rowKey.append(aid).append("_").append(day);

            Put put = new Put(Bytes.toBytes(rowKey.toString()));

            if(Utils.isNotEmpty(pkgname)){
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("pkgname"),Bytes.toBytes(pkgname));
            }
            if(Utils.isNotEmpty(uptimestr)){
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("uptime"),Bytes.toBytes(uptimestr));
            }
            if(Utils.isNotEmpty(typestr)){
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("type"),Bytes.toBytes(typestr));
            }
            if(Utils.isNotEmpty(country)){
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("country"),Bytes.toBytes(country));
            }
            if(Utils.isNotEmpty(gpcategory)){
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("gpcategory"),Bytes.toBytes(gpcategory));
            }

            k.set(Bytes.toBytes(rowKey.toString()));
            context.write(k,put);

        }

        //写入ImmutableBytesWritable, Put

    }

    @Override
    public Job getJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf,getJobNameWithTaskId());

        job.setJarByClass(Orc2HfileWS.class);
        job.setMapperClass(Orc2HfileWSMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setInputFormatClass(OrcNewInputFormat.class);

//        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job,getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job,getJobOutputPath(getJobNameWithTaskId()));

        Configuration hbaseConfig = HBaseConfiguration.create(job.getConfiguration());
        Connection connection = ConnectionFactory.createConnection(hbaseConfig);

        HTable table = (HTable)connection.getTable(TableName.valueOf(conf.get(Constants.HBASE_TABLE_NAME_ATTR)));

        //通过该方法，把生成hfile文件的配置
        HFileOutputFormat2.configureIncrementalLoad(job,table.getTableDescriptor(),table.getRegionLocator());

        return job;
    }

    @Override
    public String getJobName() {
        return "Orc2HFileWSJob";
    }
}
