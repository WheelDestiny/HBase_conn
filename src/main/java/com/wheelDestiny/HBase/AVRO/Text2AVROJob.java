package com.wheelDestiny.HBase.AVRO;

import com.wheelDestiny.HBase.Util.JobRunResult;
import com.wheelDestiny.HBase.Util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Text2AVROJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Text2AVROJob(), args));
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        JobControl jobControl = new JobControl("Text2AVROJob");

        Text2AVRO text2AVRO = new Text2AVRO();
        text2AVRO.setConf(conf);

        AVRO2Orc avro2Orc = new AVRO2Orc();

        ControlledJob test2avroJob = text2AVRO.getControlledJob();

        ControlledJob avro2orcJob = avro2Orc.getControlledJob();

        //avro2orc 依赖 text2avro
        avro2orcJob.addDependingJob(test2avroJob);

        jobControl.addJob(avro2orcJob);
        jobControl.addJob(test2avroJob);

        JobRunResult result = JobRunUtil.run(jobControl);

        result.print(true);

        return 0;
    }
}
