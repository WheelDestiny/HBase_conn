package com.wheelDestiny.HBase.MR;

import com.wheelDestiny.HBase.MR.Base.BaseMR;
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

public class Orc2Hfile0927 extends BaseMR {

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
    public static class Orc2HfileMapper0927 extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put>{

        //创建ORCUtil对象
        OrcUtil orcUtil = new OrcUtil();

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

        ImmutableBytesWritable k = new ImmutableBytesWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            orcUtil设置读文件格式
            orcUtil.setOrcTypeReadSchema(OrcFormat.SCHEMA0927);
        }

        //读取ORC数据
        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            //读取orc数据
            //struct文件结构
            //SCHEMA = "struct<uid:string,req_time:string,req_url:string>"
            String uid = orcUtil.getOrcData(value, "uid");
            String req_time = orcUtil.getOrcData(value, "req_time");
            String req_url = orcUtil.getOrcData(value, "req_url");


            //写入HBase
            //设计rowKey： aid_yyyyMMdd

            req_time = req_time.substring(0,8);
            StringBuilder rowKey = new StringBuilder();
            rowKey.append(uid).append("_").append(req_time);

            Put put = new Put(Bytes.toBytes(rowKey.toString()));

            if(Utils.isNotEmpty(req_time)){
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("req_time"),Bytes.toBytes(req_time));
            }
            if(Utils.isNotEmpty(req_url)){
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("req_url"),Bytes.toBytes(req_url));
            }



            k.set(Bytes.toBytes(rowKey.toString()));
            //写入ImmutableBytesWritable, Put
            context.write(k,put);

        }



    }

    @Override
    public Job getJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf,getJobNameWithTaskId());

        job.setJarByClass(Orc2Hfile0927.class);
        job.setMapperClass(Orc2HfileMapper0927.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setInputFormatClass(OrcNewInputFormat.class);

//        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job,getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job,getJobOutputPath(getJobNameWithTaskId()));

        Configuration hbaseConfig = HBaseConfiguration.create(job.getConfiguration());
        Connection connection = ConnectionFactory.createConnection(hbaseConfig);

        HTable table = (HTable)connection.getTable(TableName.valueOf("wheeldestiny:exam_hbase_split"));

        //通过该方法，把生成hfile文件的配置
        HFileOutputFormat2.configureIncrementalLoad(job,table.getTableDescriptor(),table.getRegionLocator());

        return job;
    }

    @Override
    public String getJobName() {
        return "Orc2HFile0927";
    }
}
