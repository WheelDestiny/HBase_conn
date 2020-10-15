package com.wheelDestiny.HBase.WeiBo.dao;

import com.wheelDestiny.HBase.WeiBo.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 1,发布微博
 * 2，删除微博
 * 3，关注用户
 * 4，取关
 * 5，获取用户微博详情
 * 6，获取某个用户的初始化页面
 */
public class HBaseDao {

    /**
     * 发布微博
     *
     * @param uid     发布用户
     * @param content 发布内容
     */
    public static void publishWeiBo(String uid, String content) throws IOException {
        //获取链接Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //step1：微博内容表
        //1，获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //2,获取当前时间戳
        long ts = System.currentTimeMillis();

        //3，获取rowKey
        String rowKey = uid + "_" + ts;

        //4,创建put对象
        Put contentPut = new Put(Bytes.toBytes(rowKey));

        //5，给put对象赋值
        contentPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(content));

        //6，执行插入数据操作
        contentTable.put(contentPut);

        //step2：向收件箱表插入数据
        //1, 获取用户关系表
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        //2, 获取发布微博的用户的关注者
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result result = relationTable.get(get);

        //3, 创建集合，用于存放微博内容表的put对象
        ArrayList<Put> inboxPuts = new ArrayList<>();

        //4, 遍历粉丝
        for (Cell cell : result.rawCells()) {
            //5, 构建微博收件箱
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));

            //6, 给收件箱表的Put对象赋值
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(rowKey));

            //7, 收件箱表Put对象存入集合
            inboxPuts.add(inboxPut);
        }
        //8, 判断是否有粉丝
        if (inboxPuts.size() > 0) {
            //9, 获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            //10, 执行收件箱表插入操作
            inboxTable.put(inboxPuts);

            //11, 关闭收件箱表
            inboxTable.close();
        }
        //关闭资源
        relationTable.close();
        inboxPuts.clone();
        connection.close();
    }

    /**
     * 关注功能
     *
     * @param uid     关注者
     * @param attends 被关注者列表
     */
    public static void addAttends(String uid, String... attends) throws IOException {
        //需要操作两个表，查询三个表
        //校验参数列表
        if (attends.length <= 0) {
            System.out.println("关注列表为空");
            return;
        }
        //获取链接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //step1，操作用户关系表
        //1,获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        //2,创建put对象
        //创建集合，存放用户关系表的put对象
        ArrayList<Put> relationPuts = new ArrayList<>();

        //3，创建操作者的put对象
        Put uidPut = new Put(Bytes.toBytes(uid));

        //4，循环创建被关注者的put对象
        for (String attend : attends) {
            //5，把关注的人添加到关注者的put对象
            Put attendPut = new Put(Bytes.toBytes(attend));
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));

            //6，把被关注者的put对象放入集合
            relationPuts.add(attendPut);

            //7，给被关注者的put对象赋值
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend), Bytes.toBytes(attend));

        }
        //8，把操作者的Put对象放入集合
        relationPuts.add(uidPut);

        //9，用户关系表的插入操作
        relationTable.put(relationPuts);

        //step2，操作收件箱表

        //获取内容表的对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //创建收件箱表的Put对象
        Put inboxPut = new Put(Bytes.toBytes(uid));

        //循环查询attends，获取每个被关注者的近期发布的微博
        for (String attend : attends) {
            //获取当前用户近期发布的微博(scan)->集合
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            ResultScanner scanner = contentTable.getScanner(scan);

            //定义一个时间戳，保证版本号不相同
            long ts = System.currentTimeMillis();

            //对获取到的结果集进行遍历
            for (Result result : scanner) {
                //可以给收件箱表的Put对象赋值
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), ts++, result.getRow());
            }
        }
        //判断当前的put对象是否为空
        if (!inboxPut.isEmpty()) {
            //如果不为空，获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            //插入数据
            inboxTable.put(inboxPut);
            //关闭收件箱表连接
            inboxTable.close();
        }

        //关闭资源
        relationTable.close();
        contentTable.close();
        connection.close();
    }

    //3,取关

    /**
     * 取消关注
     *
     * @param uid  操作人
     * @param dels 被取关列表
     * @throws IOException
     */
    public static void deleteAttention(String uid, String... dels) throws IOException {
        //获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //step1：操作用户关系表
        //获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        //创建一个集合，用户存放用户关系表的delete对象
        ArrayList<Delete> relationDelete = new ArrayList<>();

        //创建操作者的delete对象
        Delete uidDelete = new Delete(Bytes.toBytes(uid));

        //循环创建被取关者的delete对象
        for (String del : dels) {
            //给操作者的delete对象赋值
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(del));

            //创建被取关者的delete对象
            Delete delDelete = new Delete(Bytes.toBytes(del));

            //给被取关者的delete赋值
            delDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid));

            //将被取关者的delete对象添加至集合
            relationDelete.add(delDelete);
        }

        //将操作者的delete对象添加至集合
        relationDelete.add(uidDelete);

        //执行用户关系表的删除操作
        relationTable.delete(relationDelete);

        //step2:操作收件箱表
        //获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        //创建操作者的delete对象
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        //循环给操作者的delete对象赋值
        for (String del : dels) {
            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(del));
        }

        //执行收件箱表的删除操作
        inboxTable.delete(inboxDelete);

        //关闭资源
        relationTable.close();
        inboxTable.close();
        connection.close();
    }

    /**
     * 4，用户初始化数据
     *
     * @param uid
     */
    public static void getInit(String uid) throws IOException {
        //获取链接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //获取inbox表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        //获取content表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //创建inbox表get对象，设置最大版本
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result inboxResult = inboxTable.get(inboxGet);

        //并获取数据，遍历整个结果集
        for (Cell cell : inboxResult.rawCells()) {
            //构建content表get对象
            Get contentGet = new Get(CellUtil.cloneValue(cell));

            //获取具体数据内容
            Result contentResult = contentTable.get(contentGet);

            //解析内容
            for (Cell contentCell : contentResult.rawCells()) {
                System.out.println(
                        "RK" + Bytes.toString(CellUtil.cloneRow(contentCell)) +
                                ",CF" + Bytes.toString(CellUtil.cloneFamily(contentCell)) +
                                ",CN" + Bytes.toString(CellUtil.cloneQualifier(contentCell)) +
                                ",VL" + Bytes.toString(CellUtil.cloneValue(contentCell))
                );
            }
        }

        //关闭资源
        inboxTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * 获取某个人的全部微博
     * @param uid   被查询人
     */
    public static void getWeiBo(String uid) throws IOException {
        //获取链接
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //获取content表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //构建scan对象
        Scan scan = new Scan();

        //构建过滤器
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(uid+"_"));

        scan.setFilter(rowFilter);

        //获取数据
        ResultScanner result = contentTable.getScanner(scan);

        //解析数据
        result.forEach(res -> {
            for (Cell cell : res.rawCells()) {
                cell.getTimestamp();
                System.out.println(
                        "RK" + Bytes.toString(CellUtil.cloneRow(cell)) +
                                ",CF" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                                ",CN" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                                ",VL" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        });

        //关闭资源
        contentTable.close();
        connection.close();
    }
}
