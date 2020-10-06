package com.wheelDestiny.HBase.MR.Driver;

import com.wheelDestiny.HBase.AVRO.Text2AVROJob;
import com.wheelDestiny.HBase.MR.Orc2HFileJob;
import com.wheelDestiny.HBase.MR.Orc2HFileJob0927;
import com.wheelDestiny.HBase.O2HWS.Orc2HFileWSJob;
import com.wheelDestiny.HBase.ReadSnapShot.ReadSSJob;
import com.wheelDestiny.HBase.ScanHBase2Orc.ScanH2OJob;
import com.wheelDestiny.HBase.Shop.AccessTextToAvroJob;
import org.apache.hadoop.util.ProgramDriver;

public class Orc2HFileDriver {

    public static void main(String[] args) throws Throwable {
        ProgramDriver programDriver = new ProgramDriver();
        /**
         * wordMax
         * -Dtask.id=0912_wheelDestiny
         * -Dtask.input.dir=D:\\input
         * -Dtask.base.dir=D:\\output
         */

        /**
         * 		<name>zookeeper.znode.parent</name>
         * 		<value>/hbase1</value>
         */
        //Orc2HFile -Dtask.id=0924_wheelDestiny -Dtask.input.dir=D:\input\inputOrc -Dtask.base.dir=D:\output -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181 -Dzookeeper.znode.parent=/hbase1
        programDriver.addClass("Orc2HFile", Orc2HFileJob.class,"读取Orc文件");
        //Orc2HFileWS -Dtask.id=0924_wheelDestiny
        // -Dhbase.table.name=wheeldestiny:userSW -Dis.create.table=true
        // -Dtask.input.dir=D:\input\inputOrc -Dtask.base.dir=D:\output
        // -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181 -Dzookeeper.znode.parent=/hbase1
        programDriver.addClass("Orc2HFileWS", Orc2HFileWSJob.class,"读取Orc文件");

        //Orc2HFile0927 -Dtask.id=0927_wheelDestiny -Dtask.input.dir=D:\input\inputOrc -Dtask.base.dir=D:\output -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181 -Dzookeeper.znode.parent=/hbase1
        programDriver.addClass("Orc2HFile0927", Orc2HFileJob0927.class,"读取Orc文件0927");

        //ScanH2OJob -Dtask.id=0927_wheelDestiny -Dtask.base.dir=D:\output -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181 -Dzookeeper.znode.parent=/hbase1
        programDriver.addClass("ScanH2OJob", ScanH2OJob.class,"读取ScanH2OJob");

        //ReadSS -Dtask.id=0927_wheelDestiny -Dhbase.table.snapshot.name=userHBase_snapshot -Dhbase.snapshot.restore=/user/wheeldestiny26/hbase/restore -Dtask.base.dir=/user/wheeldestiny26/hbase/output -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181 -Dzookeeper.znode.parent=/hbase1
        programDriver.addClass("ReadSS", ReadSSJob.class,"读Snapshot");

        //Text2AVRO -Dtask.id=0928_wheelDestiny -Dtask.input.dir=D:\\input -Dtask.base.dir=D:\\output
        programDriver.addClass("Text2AVRO", Text2AVROJob.class,"读Text为AVRO文件");

        programDriver.addClass("access_dwd_avro", AccessTextToAvroJob.class, "读取access日志转avro文件");

        programDriver.run(args);

    }
}
