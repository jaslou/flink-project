package com.jaslou.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Test {

    public static Configuration conf=null;
    public static Connection conn=null;

    /**
     * 类级别的初始化，只是在类加载的时候做一次 配置zookeeper的端口2181
     * 配置zookeeper的仲裁主机名centos，如果有多个机器，主机名间用冒号隔开 配置hbase master
     * 还有一种方式是new一个configuration对象，然后用addresource方法去添加xml配置文件 但是像这样显式的配置是会覆盖xml里的配置的
     */
    static {
        conf = HBaseConfiguration.create();
        conf.set("zookeeper.znode.parent", "/hbase");
        conf.set("hbase.zookeeper.quorum","192.168.1.111");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.setInt("hbase.rpc.timeout",2000);
        conf.setInt("hbase.client.scanner.timeout.period",2000);

        try {

            conn = ConnectionFactory.createConnection(conf);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        HbaseMain.createTable("std","info1");
    }


    public static  void createtable() throws IOException {
        String tableName="ns2:student";
        String[] columnFamilys=new String[]{"data"};


        Admin admin = conn.getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        for (String family : columnFamilys) {
            HColumnDescriptor columnfamily = new HColumnDescriptor(family);
            table.addFamily(columnfamily);
        }

        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("Table Exists");
        } else {
            admin.createTable(table);
            System.out.println("Table Created");
            admin.close();
        }
    }
}