import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDemo {

    private static Configuration conf = HBaseConfiguration.create();

    static {

        conf.set("hbase.rootdir", "hdfs://master2:9000/hbase");

        // 设置Zookeeper,直接设置IP地址
        conf.set("hbase.zookeeper.quorum", "master2");

    }


    // 创建表

    public static void createTable(String tablename, String columnFamily) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);

        Admin admin = connection.getAdmin();


        TableName tableNameObj = TableName.valueOf(tablename);


        if (admin.tableExists(tableNameObj)) {

            System.out.println("Tableexists!");

            System.exit(0);

        } else {

            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));

            tableDesc.addFamily(new HColumnDescriptor(columnFamily));

            admin.createTable(tableDesc);

            System.out.println("createtable success!");

        }

        admin.close();

        connection.close();

    }


    // 删除表


    public static void deleteTable(String tableName) {

        try {

            Connection connection = ConnectionFactory.createConnection(conf);

            Admin admin = connection.getAdmin();

            TableName table = TableName.valueOf(tableName);

            admin.disableTable(table);

            admin.deleteTable(table);

            System.out.println("deletetable " + tableName + "ok.");

        } catch (IOException e) {

            e.printStackTrace();

        }

    }

    // 插入一行记录
    public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) {

        try {

            Connection connection = ConnectionFactory.createConnection(conf);

            Table table = connection.getTable(TableName.valueOf(tableName));

            Put put = new Put(Bytes.toBytes(rowKey));

            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));

            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));

            table.put(put);

            table.close();

            connection.close();

            System.out.println("insertrecored " + rowKey + " totable " + tableName + "ok.");

        } catch (IOException e) {

            e.printStackTrace();

        }

    }


    public static void main(String[] args) throws Exception {

        HbaseDemo.createTable("testTb", "info");

        //HbaseDemo.addRecord("testTb", "001", "info", "name", "zhangsan");

        //HbaseDemo.addRecord("testTb", "001", "info", "age", "20");

        //HbaseDao.deleteTable("testTb");

    }

}