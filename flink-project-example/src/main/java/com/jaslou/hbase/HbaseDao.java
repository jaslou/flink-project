package com.jaslou.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDao {

    // 发布微博
    public static void publishWeibo (String uid, String content) throws IOException {
        // 获取Connection对象
        Connection connection = HbaseMain.getConnection();

        // 第一部分：操作微博内容（content）
        // 1. 获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(HbaseConstants.CONTENT_TABLE));

        //2. 获取时间戳
        long ts = System.currentTimeMillis();

        //3. 拼接rowKey
        String rowKey = uid + "_" + ts;

        //4. 创建Put对象，并赋值
        Put contentPut = new Put(Bytes.toBytes(rowKey));
        contentPut.addColumn(Bytes.toBytes(HbaseConstants.CONTENT_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(content));

        //5. 插入数据
        contentTable.put(contentPut);

        // 第二部分：操作微博收件箱表（inbox）
        //1. 获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(HbaseConstants.RELATION_TABLE));

        //2. 获取当前发布微博人的fans列簇数据
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(HbaseConstants.RELATION_TABLE_CF2));
        Result relationResult = relationTable.get(get);

        //3. 创建存放微博收件箱表的put对象
        List<Put> putList = new ArrayList<>();
        for (Cell cell : relationResult.rawCells()) {
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(HbaseConstants.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            putList.add(inboxPut);
        }

        if (putList.size() > 0) {
            // 4. 获取收件箱表的对象（inbox），并执行插入数据
            Table inboxTable = connection.getTable(TableName.valueOf(HbaseConstants.INBOX_TABLE));
            inboxTable.put(putList);
            inboxTable.close();
        }
        relationTable.close();
        contentTable.close();
    }

    // 关注用户
    public static void addAttends(String uid, String... attends) throws IOException {
        // 校验是否添加了待关注的人
        if (attends.length <= 0) {
            System.out.println("请选择待关注的人");
        }

        // 获取Connection对象
        Connection connection = HbaseMain.getConnection();

        //第一部分：操作用户关系表
        //1. 获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(HbaseConstants.RELATION_TABLE));

        //2.创建集合用于存放用户关系表的Put对象
        List<Put> resultPut = new ArrayList<>();

        //3. 创建操作者的Put对象
        Put uidPut = new Put(Bytes.toBytes(uid));

        //4. 循环创建待关注者的对象
        for (String attend : attends) {
            // 5. 给操作者的Put对象赋值
            uidPut.addColumn(Bytes.toBytes(HbaseConstants.RELATION_TABLE_CF1), Bytes.toBytes(attend), Bytes.toBytes(attend));

            //6. 给被关注着的Put对象赋值
            Put relationPut = new Put(Bytes.toBytes(attend));
            relationPut.addColumn(Bytes.toBytes(HbaseConstants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));

            //7. 将被关注者的Put对象放入集合
            resultPut.add(relationPut);
        }

        //8.将操作者的Put对象放入集合
        resultPut.add(uidPut);

        //第二部分：操作收件箱表
        //1. 获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(HbaseConstants.CONTENT_TABLE));

        //2. 创建收件箱表的Put对象
        Put inboxPut = new Put(Bytes.toBytes(uid));

        //3. 循环，获取每个被关注者的近期发布的微博
        for (String attend : attends) {
            //4. 获取当前被关注着的近期发布的微博（scan）
            Scan attendScan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            ResultScanner resultScanner = contentTable.getScanner(attendScan);
            long ts = System.currentTimeMillis();
            for (Result result : resultScanner) {
                //5. 给收件箱表的Put对象赋值,控制时间戳不一致，不然加到的结果是一条
                inboxPut.addColumn(Bytes.toBytes(HbaseConstants.INBOX_TABLE_CF), Bytes.toBytes(attend), ts++, result.getRow());
            }
        }

        if (!inboxPut.isEmpty()) {
            // 获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(HbaseConstants.INBOX_TABLE));
            inboxTable.put(inboxPut);
            inboxTable.close();
        }

        contentTable.close();
        relationTable.close();
    }

    // 取关
    public static void deleteAttends(String uid, String... attends) throws IOException {
        // 获取Connection对象
        Connection connection = HbaseMain.getConnection();

        // 第一部分：操作用户关系表
        //1. 获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(HbaseConstants.RELATION_TABLE));

        //2. 创建集合用于存放用户关系表的delete对象
        List<Delete> deleteList = new ArrayList<>();

        //3. 创建操作者的delete对象
        Delete uidDelete = new Delete(Bytes.toBytes(uid));

        //4. 循环创建被取关者的delete对象
        for (String attend : attends) {
            //5. 给操作者的delete的对象赋值
            uidDelete.addColumns(Bytes.toBytes(HbaseConstants.RELATION_TABLE_CF1), Bytes.toBytes(attend));

            Delete delete = new Delete(Bytes.toBytes(attend));
            //6. 给被取关者的delete对象赋值
            delete.addColumn(Bytes.toBytes(HbaseConstants.RELATION_TABLE_CF2), Bytes.toBytes(uid));

            //7. 将被取关者的delete对象添加至集合
            deleteList.add(delete);
        }
        // 将操作者的delete对象添加到集合
        deleteList.add(uidDelete);

        // 执行删除操作
        relationTable.delete(deleteList);

        // 第二部分：操作收件箱表
        //1. 获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(HbaseConstants.INBOX_TABLE));

        //2. 创建操作者的delete对象
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        //3. 循环给操作者的Delete对象赋值
        for (String attend : attends) {
            inboxDelete.addColumn(Bytes.toBytes(HbaseConstants.INBOX_TABLE_CF), Bytes.toBytes(attend));
        }

        //4. 执行删除操作
        inboxTable.delete(inboxDelete);
        inboxTable.close();
        relationTable.close();
    }

    // 获取某个人的初始化页面数据
    public static void getInit(String uid) throws IOException {
        // 获取Connection对象
        Connection connection = HbaseMain.getConnection();

        //1. 获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(HbaseConstants.INBOX_TABLE));

        //2. 获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(HbaseConstants.CONTENT_TABLE));

        //3. 创建收件箱表Get对象，并获取数据（设置最大版本）
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);

        //4. 遍历获取的数据
        for (Cell cell : result.rawCells()) {
            //5. 构建微博内容表Get对象
            Get contentGet = new Get(CellUtil.cloneValue(cell));

            //6. 获取给get对象的数据
            Result contentResult = contentTable.get(contentGet);

            //7. 解析并打印
            for (Cell rawCell : contentResult.rawCells()) {
                System.out.println("RK：" + Bytes.toString(CellUtil.cloneRow(rawCell))
                        + "，CF："  + Bytes.toString(CellUtil.cloneFamily(rawCell))
                        + "，CN：" + Bytes.toString(CellUtil.cloneQualifier(rawCell))
                        + "，value：" + Bytes.toString(CellUtil.cloneValue(rawCell)));
            }
        }
        inboxTable.close();
        contentTable.close();
    }

    // 获取某个人的微博内容
    public static void getContent(String uid) throws IOException {
        // 获取Connection对象
        Connection connection = HbaseMain.getConnection();

        //1. 获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(HbaseConstants.CONTENT_TABLE));

        Scan scan = new Scan();

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new SubstringComparator(uid + "_"));
        scan.setFilter(rowFilter);

        ResultScanner scanner = contentTable.getScanner(scan);

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RK：" + Bytes.toString(CellUtil.cloneRow(cell))
                        + "，CF："  + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "，CN：" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "，value：" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        contentTable.close();
    }

}
