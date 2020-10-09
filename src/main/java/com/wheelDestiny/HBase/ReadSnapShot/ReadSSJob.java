package com.wheelDestiny.HBase.ReadSnapShot;

import com.google.inject.internal.cglib.core.$AbstractClassGenerator;
import com.wheelDestiny.HBase.Util.Constants;
import com.wheelDestiny.HBase.Util.JobRunResult;
import com.wheelDestiny.HBase.Util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadSSJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        //获取配置对象
        Configuration conf = getConf();

        String tableName = conf.get(Constants.HBASE_TABLE_NAME_ATTR);

        JobControl jobControl = new JobControl("ReadSSJob");

        ReadSS readSS = new ReadSS();
        readSS.setConf(conf);

        ControlledJob controlledJob = readSS.getControlledJob();

        jobControl.addJob(controlledJob);

        JobRunResult run = JobRunUtil.run(jobControl);


        // ----【注意】 集成的hbase导入代码，需要在集群上运行---------------
        // 执行完任务链上的任务，可以把输出的hfile文件，集成导入到hbase表
        // 参考：hadoop jar /usr/local/hbase/lib/hbase-shell-1.3.1.jar
//		completebulkload /user/hadoop/hbase/orc2hfile/orc2hfile_0925_panniu panniu:user_hbase

        // hfile输出path：/user/hadoop/hbase/orc2hfile/orc2hfile_0925_panniu

//        Path jobOutputPath = readSS.getJobOutputPath(readSS.getJobNameWithTaskId());
//
//        String[] inputParams ={jobOutputPath.toString(),tableName};

        run.print(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ReadSSJob(),args));
    }
}
