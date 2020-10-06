package com.wheelDestiny.HBase.MR.Base;

import com.wheelDestiny.HBase.Util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
abstract public class BaseMR {
    private static Configuration conf;

    /**
     * 设置公共Configuration
     * @param cconf
     */
    public void setConf(Configuration cconf){
        conf = cconf;
    }

    public ControlledJob getControlledJob() throws IOException {
        //创建ControlledJob对象
        ControlledJob cjob = new ControlledJob(conf);
        //创建WordCount运行的job对象和配置job参数


        //加载mapreduce用的配置，生成mapreduce的Job对象

        FileSystem fileSystem = FileSystem.get(conf);
        Path outPath = getJobOutputPath(getJobNameWithTaskId());
        if(fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }

        //为了每个任务的job配置不相互影响，需要复制公共的Configuration对象数据，放入getJob()方法里
        Configuration jobConf = new Configuration();
        for (Map.Entry<String, String> entry : conf) {
            String key = entry.getKey();
            String value = entry.getValue();
            jobConf.set(key,value);
        }

        Job job = getJob(jobConf);


        //把任务运行的job对象和ControlledJob对象关联
        cjob.setJob(job);
        return cjob;
    }

    /**
     * 定义一个抽象方法，由子类实现
     * 实现每个任务job对象以及配置job的参数
     * @param conf
     * @return 配置好的Job对象
     */
    public abstract Job getJob(Configuration conf) throws IOException;

    /**
     * 每个任务的任务名称，比如wordCount之类的
     * @return 任务名称
     */
    public abstract String getJobName();

    /**
     * 获取根据-D参数生成个性化的任务名称
     * @return
     */
    public String getJobNameWithTaskId(){
        return getJobName()+"_"+conf.get(Constants.TASK_ID_ATTR);
    }

    /**
     * 获取首个输入目录
     * @return
     */
    public Path getFirstJobInputPath(){
        return new Path(conf.get(Constants.TASK_INPUT_DIR_ATTR));
    }

    /**
     * 每个任务个性化的输出目录
     * @param jobName
     * @return
     */
    public Path getJobOutputPath(String jobName){
        return new Path(conf.get(Constants.TASK_BASE_DIR_ATTR)+"/"+jobName);
    }

}
