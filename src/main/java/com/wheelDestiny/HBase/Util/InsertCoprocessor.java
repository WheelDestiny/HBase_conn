package com.wheelDestiny.HBase.Util;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * 协处理器
 * 继承BaseRegionObserver
 * 重写方法：postPut
 * 实现逻辑
 *      增加emp的数据，同时增加empbak的数据
 *
 * 将项目打包为依赖包传到hbase服务器上
 */
public class InsertCoprocessor extends BaseRegionObserver {

    //prePut
    //doPut
    //postPot
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //table
        //获取表对象
        Table table = e.getEnvironment().getTable(TableName.valueOf("wheeldestiny:empbak"));

        //table.put()
        table.put(put);

        //关闭table
        table.close();

    }
}
