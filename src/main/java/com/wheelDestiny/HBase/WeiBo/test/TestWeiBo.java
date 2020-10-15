package com.wheelDestiny.HBase.WeiBo.test;

import com.wheelDestiny.HBase.WeiBo.constants.Constants;
import com.wheelDestiny.HBase.WeiBo.dao.HBaseDao;
import com.wheelDestiny.HBase.WeiBo.utils.HBaseUtil;

import java.io.IOException;

public class TestWeiBo {

    public static void init(){
        try {
            //创建命名空间
            HBaseUtil.createNameSpace(Constants.NAMESPACE);

            //创建content表
            HBaseUtil.createTable(Constants.CONTENT_TABLE,Constants.CONTENT_TABLE_VERSION,Constants.CONTENT_TABLE_CF);

            //创建relation表
            HBaseUtil.createTable(Constants.RELATION_TABLE,
                    Constants.RELATION_TABLE_VERSION,
                    Constants.RELATION_TABLE_CF1,
                    Constants.RELATION_TABLE_CF2);

            //创建inbox表
            HBaseUtil.createTable(Constants.INBOX_TABLE,
                    Constants.INBOX_TABLE_VERSION,
                    Constants.INBOX_TABLE_CF);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
//        init();

        //1001发布微博
//        HBaseDao.publishWeiBo("1001","第一条微博");

        //1002关注1001,1003
//        HBaseDao.addAttends("1002","1001","1003");

        //获取1002的初始化页面
//        HBaseDao.getInit("1002");
//
//        System.out.println("----------1-----------");
//
//        //1003发布3条微博，同时1001发布两条微博
////        HBaseDao.publishWeiBo("1003","第一条微博");
////        HBaseDao.publishWeiBo("1003","第2条微博");
////        HBaseDao.publishWeiBo("1003","第3条微博");
////        HBaseDao.publishWeiBo("1001","第2条微博");
////        HBaseDao.publishWeiBo("1001","第3条微博");
//
//        //获取1002的初始化页面
//        HBaseDao.getInit("1002");
//
//        System.out.println("----------2-----------");
//
        //1002取关1003
//        HBaseDao.deleteAttention("1002","1003");
//        //获取1002的初始化页面
//        HBaseDao.getInit("1002");
//
//        System.out.println("----------3-----------");
//
//        //1002关注1003
//        HBaseDao.addAttends("1002","1003");
//        //获取1002的初始化页面
//        HBaseDao.getInit("1002");
//
//        System.out.println("----------4-----------");
        //获取1001微博详情
        HBaseDao.getWeiBo("1001");



    }

}
