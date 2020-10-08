package com.wheelDestiny.HBase.O2HWS;

import com.wheelDestiny.HBase.MR.Orc2Hfile;
import com.wheelDestiny.HBase.Util.Constants;
import com.wheelDestiny.HBase.Util.JobRunResult;
import com.wheelDestiny.HBase.Util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Orc2HFileWSJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Orc2HFileWSJob(), args));
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        JobControl jobControl = new JobControl("Orc2HFileJob");

        Orc2HfileWS orc2HfileWS = new Orc2HfileWS();
        orc2HfileWS.setConf(conf);

        ControlledJob orcJob = orc2HfileWS.getControlledJob();

        String tableName = conf.get(Constants.HBASE_TABLE_NAME_ATTR);
        if("true".equals(conf.get(Constants.IS_CREATE_HBASE_TABLE_NAME_ATTR))){
            String splitClassName = "com.wheelDestiny.HBase.Util.TableRegionSplit";
            //创建预分区
            String[] createSplitTableParams = {tableName,splitClassName,"-c","2","-f","cf"};
            RegionSplitter.main(createSplitTableParams);
        }

        jobControl.addJob(orcJob);

        JobRunResult result = JobRunUtil.run(jobControl);

        //注！！！ 集成的hbase代码，需要在集群上运行
        //执行完任务链上的任务，可以吧输出的HFile文件，集成导入到hbase表，
        // 参考org\apache\hadoop\hbase\mapreduce\Driver.java

        //completebulkload /user/wheeldestiny26/output/Orc2HFileJob_0924_wheelDestiny wheeldestiny:userHBase
        Path jobOutputPath = orc2HfileWS.getJobOutputPath(orc2HfileWS.getJobNameWithTaskId());
        String[] inputParams = {jobOutputPath.toString(),tableName};

        LoadIncrementalHFiles.main(inputParams);


        result.print(true);

        return 0;
    }
}
