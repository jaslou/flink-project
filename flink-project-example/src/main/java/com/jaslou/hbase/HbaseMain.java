package com.jaslou.hbase;

import com.jaslou.util.PropertyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class HbaseMain {

    private static Admin admin = null;
    private static Connection connection = null;

    static Logger logger = LoggerFactory.getLogger(HbaseMain.class);

    // initialize connection and admin
    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", PropertyUtil.getStrValue("hbase.rootdir"));
        configuration.set("hbase.zookeeper.quorum", PropertyUtil.getStrValue("hbase.zookeeper.quorum"));
        configuration.set("hbase.client.scanner.timeout.period", PropertyUtil.getStrValue("hbase.client.scanner.timeout.period"));
        configuration.set("hbase.rpc.timeout", PropertyUtil.getStrValue("hbase.rpc.timeout"));
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        return connection;
    }



    // create a Hbase table
    public static void createTable(String tableName, String... columnFamilyNames) throws Exception {
        TableName table = TableName.valueOf(tableName);
        if(admin.tableExists(table)) {
            logger.info("### table {} is exists", tableName);
        }else {
            logger.info("### Start create HTable {}", tableName);
            HTableDescriptor descriptor =new HTableDescriptor(table);
            for(String columnFamily: columnFamilyNames) {
                descriptor.addFamily(new HColumnDescriptor(columnFamily));
            }
            try {
                admin.createTable(descriptor);
                assertTrue("Fail to create the table", admin.tableExists(table));
                logger.info("### Create HTable success {}", tableName);
            } catch (IOException e) {
                logger.info("### Create HTable failed {}", tableName);
            }
        }
    }

    // Open a Hbase Table
    public static HTable getTable(String tableName) throws IOException {
        HTable table = (HTable) admin.getConnection().getTable(TableName.valueOf(tableName));
        if(table == null) return null;
        return table;
    }

    // get a Hbase Table
    public static HTable getTable(TableName tableName) throws Exception {
        HTable table = (HTable) admin.getConnection().getTable(tableName);
        if(table == null) return null;
        return table;
    }

    // True if table exists already.
    public static boolean isExistsTable(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    // delete a table
    public static void deleteHTables(String... tableNames) throws Exception {
        if(admin != null) {
            for(String tableName : tableNames) {
                TableName tn = TableName.valueOf(tableName);
                if(admin.tableExists(tn)) {
                    admin.disableTable(tn);
                    admin.deleteTable(tn);
                }
            }
        }
    }

    // 关闭资源
    public static void close() {
        if(admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 添加数据
     * @param tableName 表名
     * @param rowKey 行号
     * @param familyName 列族名
     * @param column 列名
     * @param data 数据
     * @throws throws Thrown if checks for existence and sanity fail.
     */
    public static void putData(String tableName, String rowKey, String familyName, String column, String data) throws Exception{
        Table table = getTable(tableName);
        if(table != null) {
            Put put = new Put(rowKey.getBytes());
            put.addColumn(familyName.getBytes(), column.getBytes(), data.getBytes());
            table.put(put);
        }
        if (table != null) {
            table.close();
        }
    }

    /**
     * 获取表中所有keys
     * @param tableName 表名
     * @return return the list
     * @throws Exception Thrown, if the program executions fails.
     */
    public static List<String> getAllKey(String tableName) throws Exception {
        List<String> keys = new ArrayList<>();
        Table table = getTable(tableName);
        Scan scan = new Scan();
        if(table != null) {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result: scanner) {
                keys.add(result.getRow().toString());
            }
        }
        if (table != null ){
            table.close();
        }
        return keys;
    }

    /**
     * 获取某个tableName的所有rowKey
     * @param tableName the table name
     * @param rowKey rowKey
     * @return return the list
     * @throws Exception Thrown, if the program executions fails.
     */
    public static List<Row> getRow(String tableName, String rowKey) throws Exception {
        List<Row> keys = new ArrayList<>();
        Table table = getTable(tableName);
        if (table != null) {
            Get get = new Get(rowKey.getBytes());
            Result result = table.get(get);
        }
        if (table != null) {
            table.close();
        }
        return keys;
    }

    /**
     *
     * @param tableName tableName
     * @param familyName familyName
     * @param rowKey rowKey
     * @param column column
     * @return 获取单个RowKey对应的数据数据
     * @throws Exception Thrown, if the program executions fails.
     */
    public static Map<String, String> getData(String tableName, String rowKey, String familyName, String column) throws IOException  {
        Map<String, String> resultMap = new HashMap<>();
        // 1. 获取表对象
        Table table = getTable(tableName);
        if (table != null) {
            Get get = new Get(rowKey.getBytes());
            get.addColumn(familyName.getBytes(), column.getBytes());
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                String rk = Bytes.toString(CellUtil.cloneRow(cell));
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String cn = Bytes.toString(CellUtil.cloneQualifier(cell));
                String key = "RK：" + rk  +"，CF：" + cf + "，CN：" + cn;
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(key + "，value：" + value);
                resultMap.put(key, value);
            }
        }
        if (table != null) {
            table.close();
        }
        return resultMap;
    }

    public static Map<String, String> scanTable(String tableName) throws IOException {
        // 1. 获取表对象
        Table table = getTable(tableName);
        Map<String, String> resultMap = new HashMap<>();
        if (table != null) {
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    String rk = Bytes.toString(CellUtil.cloneRow(cell));
                    String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                    String cn = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String key = "RK：" + rk  +"，CF：" + cf + "，CN：" + cn;
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println(key + "，value：" + value);
                    resultMap.put(key, value);
                }
            }
        }
        if (table != null) {
            table.close();
        }
        return resultMap;
    }

    public static void main(String[] args) throws Exception {

//        scanTable("student");
            getData("student","1002", "info2", "address");
//        createTable("student2", "info", "info2");
        putData("student", "1001", "info1","phone","152*******");

        close();

    }

}

